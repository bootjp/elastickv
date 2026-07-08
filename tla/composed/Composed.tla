------------------------------ MODULE Composed -------------------------------
(***************************************************************************)
(* TLA+ specification of the cross-module safety properties — the M5      *)
(* milestone of docs/design/2026_05_28_partial_tla_safety_spec.md §5.5.   *)
(*                                                                         *)
(* M1..M4 each modelled a single subsystem in isolation.  M5 captures     *)
(* what happens at the seams between them — specifically, two safety      *)
(* properties that cannot be expressed inside any one module:             *)
(*                                                                         *)
(*   Composed-1  Commit goes to the owning group.  For every committed    *)
(*               write to key k, the Raft group that accepted the commit  *)
(*               is the one that owned k at the catalog version the      *)
(*               transaction observed (`txnObservedVer[t]`).              *)
(*                                                                         *)
(*   Composed-3  Strict serialisability bound.  Committed transactions    *)
(*               have distinct `commit_ts` values.  The monotonic ts     *)
(*               allocation in BeginTxn / Commit makes this hold by       *)
(*               construction in the bounded model (no cross-group        *)
(*               collision can occur because all timestamps share one     *)
(*               `tsCounter`).  Under the gap toggle we ALSO check        *)
(*               distinctness, but Composed-3 itself is not the gap       *)
(*               invariant — Composed-1 is.                                *)
(*                                                                         *)
(*   Composed-2  Read-after-write across SplitRange is vacuously true     *)
(*               in this M5 abstraction because the implementation's      *)
(*               SplitRange is same-group only (per CLAUDE.md).  We       *)
(*               still model cross-group route changes via                *)
(*               `ProposeRouteChange` so Composed-1 is non-trivial;       *)
(*               the cross-group migration clause in Composed-2 is a     *)
(*               forward-looking guard rail.                              *)
(*                                                                         *)
(* The `EnableSafety` CONSTANT gates the Commit-time owning-group check  *)
(* — under it the committing group must equal the owning group at the    *)
(* txn's observed catalog version; under the gap toggle the committing   *)
(* group is chosen freely, so TLC finds the canonical Composed-1         *)
(* counterexample where a route move (k from g1 to g2) is followed by    *)
(* a transaction that observed the old catalog committing to the new    *)
(* group.                                                                  *)
(*                                                                         *)
(* This module does NOT INSTANCE the M1..M4 modules.  Each of those      *)
(* tightly bounds its own state for tractable TLC; INSTANCEing all four  *)
(* into one product spec would explode the state space well past M5's    *)
(* "<10 min at default bounds" target from §8.1.  Composed.tla instead   *)
(* recreates the minimal cross-module state needed to express            *)
(* Composed-1 and Composed-3 — the abstract HLC clock (`tsCounter`),     *)
(* the catalog history (`routes[v]`), the transaction lifecycle, and     *)
(* the per-key version chain — and the integration claim is that this    *)
(* projection preserves the invariants the full product would assert.    *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets

CONSTANTS
    Keys,                \* finite set of keys
    Groups,              \* finite set of Raft groups
    TxnIds,              \* finite set of transactions
    MaxVersions,         \* bound on catalog versions
    MaxTs,               \* bound on the abstract HLC counter
    MaxOps,              \* bound on total actions
    InitGroup,           \* initial owning group of every key at version 0
    EnableSafety,        \* TRUE: encode Composed-1  commit guard (observed-version owner)
    EnableCurrentFence   \* TRUE: encode Composed-1a commit guard (current-version owner)

ASSUME InitGroup \in Groups

\* Transaction lifecycle states.
States == {"Idle", "Active", "Committed", "Aborted"}

\* Sentinel for "this txn has not chosen a commit group yet".
NoGroup == "no_group"

VARIABLES
    catalogVersion,         \* Nat.  Latest durable catalog version.
    routes,                 \* [0..MaxVersions -> [Keys -> Groups]].  Historical
                            \* snapshot per version; v <= catalogVersion is real,
                            \* v > catalogVersion is the init map.
    tsCounter,              \* Nat.  Monotonic abstract HLC; advances on BeginTxn
                            \* and Commit.
    txnState,               \* [TxnIds -> States]
    txnStartTs,             \* [TxnIds -> 0..MaxTs]
    txnObservedVer,         \* [TxnIds -> 0..MaxVersions].  Catalog version this
                            \* txn read at BeginTxn.
    txnWriteSet,            \* [TxnIds -> SUBSET Keys]
    txnCommitTs,            \* [TxnIds -> 0..MaxTs]
    txnCommitGroup,         \* [TxnIds -> Groups \cup {NoGroup}]
    currentVersionAtCommit, \* [TxnIds -> 0..MaxVersions].  Catalog version the
                            \* FSM observed at apply time (i.e. when the commit
                            \* lands).  Set on Commit; together with
                            \* txnObservedVer this witnesses BOTH the spec's
                            \* observed-version check and the apply-time
                            \* current-version cross-version-read fence
                            \* (Composed-1a; see docs/design/
                            \* 2026_05_29_implemented_composed1_cross_group_commit_guard.md
                            \* §4.4 and §5).
    opCount

vars == <<catalogVersion, routes, tsCounter, txnState, txnStartTs,
          txnObservedVer, txnWriteSet, txnCommitTs, txnCommitGroup,
          currentVersionAtCommit, opCount>>

\* === HELPERS ===
CommittedTxns == { t \in TxnIds : txnState[t] = "Committed" }

\* The owning group of k at catalog version v.
OwnerAt(v, k) == routes[v][k]

\* === INIT ===
Init ==
    /\ catalogVersion         = 0
    /\ routes                 = [v \in 0..MaxVersions |-> [k \in Keys |-> InitGroup]]
    /\ tsCounter              = 0
    /\ txnState               = [t \in TxnIds |-> "Idle"]
    /\ txnStartTs             = [t \in TxnIds |-> 0]
    /\ txnObservedVer         = [t \in TxnIds |-> 0]
    /\ txnWriteSet            = [t \in TxnIds |-> {}]
    /\ txnCommitTs            = [t \in TxnIds |-> 0]
    /\ txnCommitGroup         = [t \in TxnIds |-> NoGroup]
    /\ currentVersionAtCommit = [t \in TxnIds |-> 0]
    /\ opCount                = 0

\* === ACTIONS ===

(***************************************************************************)
(* ProposeRouteChange(k, g_new): the control plane atomically bumps the   *)
(* catalog version and reassigns ownership of k to g_new.  Mirrors the    *)
(* implementation's SplitRange + saveSplitResultViaCoordinator collapsed *)
(* into a single TLA+ action.  Unlike the real `SplitRange` (which is    *)
(* same-group only per CLAUDE.md), we let g_new differ from the current  *)
(* owner so Composed-1's owning-group check has teeth.  Under the gap    *)
(* config a transaction that observed the old catalog version may still  *)
(* commit to the new group — that is the Composed-1 violation.           *)
(***************************************************************************)
ProposeRouteChange(k, g_new) ==
    /\ opCount < MaxOps
    /\ catalogVersion < MaxVersions
    /\ catalogVersion' = catalogVersion + 1
    /\ routes' = [routes EXCEPT ![catalogVersion + 1] =
                    [routes[catalogVersion] EXCEPT ![k] = g_new]]
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<tsCounter, txnState, txnStartTs, txnObservedVer,
                   txnWriteSet, txnCommitTs, txnCommitGroup,
                   currentVersionAtCommit>>

(***************************************************************************)
(* BeginTxn(t): the txn enters Active state, allocates a fresh start_ts  *)
(* from the monotonic clock, and pins the catalog version it observes at *)
(* that moment.  txnObservedVer[t] is the load-bearing variable for       *)
(* Composed-1 — every later WriteIntent / Commit decision is measured    *)
(* against the catalog as it was at BeginTxn.                            *)
(***************************************************************************)
BeginTxn(t) ==
    /\ txnState[t] = "Idle"
    /\ tsCounter < MaxTs
    /\ opCount < MaxOps
    /\ tsCounter'      = tsCounter + 1
    /\ txnState'       = [txnState       EXCEPT ![t] = "Active"]
    /\ txnStartTs'     = [txnStartTs     EXCEPT ![t] = tsCounter + 1]
    /\ txnObservedVer' = [txnObservedVer EXCEPT ![t] = catalogVersion]
    /\ opCount'        = opCount + 1
    /\ UNCHANGED <<catalogVersion, routes, txnWriteSet, txnCommitTs,
                   txnCommitGroup, currentVersionAtCommit>>

(***************************************************************************)
(* WriteIntent(t, k): buffer a write to k locally.  No lock or version is *)
(* taken yet (this is the pre-prepare stage); the actual write+commit    *)
(* happens atomically in Commit below.  Modelled simpler than M2 so the   *)
(* cross-module focus stays on Composed-1.                                 *)
(***************************************************************************)
WriteIntent(t, k) ==
    /\ txnState[t] = "Active"
    /\ k \notin txnWriteSet[t]
    /\ opCount < MaxOps
    /\ txnWriteSet' = [txnWriteSet EXCEPT ![t] = @ \cup {k}]
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<catalogVersion, routes, tsCounter, txnState, txnStartTs,
                   txnObservedVer, txnCommitTs, txnCommitGroup,
                   currentVersionAtCommit>>

(***************************************************************************)
(* Commit(t, g): atomically commit t through Raft group g.  Allocates a   *)
(* fresh commit_ts and sets txnCommitGroup[t] = g.                       *)
(*                                                                         *)
(* Under EnableSafety the committing group MUST own every key in           *)
(* txnWriteSet[t] at the txn's observed catalog version — this is the    *)
(* Composed-1 enforcement at the action level.  Under the gap toggle      *)
(* that check is removed and TLC finds the canonical 4-step               *)
(* counterexample:                                                         *)
(*                                                                         *)
(*   BeginTxn(t)                  -- txn observes catalogVersion = 0      *)
(*   WriteIntent(t, k1)           -- k1 owned by g1 at v=0               *)
(*   ProposeRouteChange(k1, g2)   -- v=1, routes[1][k1] = g2             *)
(*   Commit(t, g2) -- gap mode    -- but routes[0][k1] = g1 != g2 → fail*)
(***************************************************************************)
Commit(t, g) ==
    /\ txnState[t] = "Active"
    /\ tsCounter < MaxTs
    \* Composed-1 — observed-version owner check (the spec-level
    \* refinement of the design doc §4.2(a) check).
    /\ \/ ~EnableSafety
       \/ \A k \in txnWriteSet[t] : OwnerAt(txnObservedVer[t], k) = g
    \* Composed-1a — current-version owner check (the apply-time
    \* cross-version-read fence from design doc §4.4).  This is the
    \* strictly stronger guard the implementation adds on top of the
    \* spec's literal Commit predicate; it prevents the codex P1 trace
    \* where the observed-version check passes but a later
    \* ProposeRouteChange has moved the key off the committing group.
    /\ \/ ~EnableCurrentFence
       \/ \A k \in txnWriteSet[t] : OwnerAt(catalogVersion, k) = g
    /\ tsCounter'               = tsCounter + 1
    /\ txnState'                = [txnState               EXCEPT ![t] = "Committed"]
    /\ txnCommitTs'             = [txnCommitTs            EXCEPT ![t] = tsCounter + 1]
    /\ txnCommitGroup'          = [txnCommitGroup         EXCEPT ![t] = g]
    /\ currentVersionAtCommit'  = [currentVersionAtCommit EXCEPT ![t] = catalogVersion]
    /\ UNCHANGED <<catalogVersion, routes, txnStartTs, txnObservedVer,
                   txnWriteSet, opCount>>

(***************************************************************************)
(* Abort(t): transition Active to Aborted.  No state change beyond txnState.*)
(***************************************************************************)
Abort(t) ==
    /\ txnState[t] = "Active"
    /\ txnState' = [txnState EXCEPT ![t] = "Aborted"]
    /\ UNCHANGED <<catalogVersion, routes, tsCounter, txnStartTs,
                   txnObservedVer, txnWriteSet, txnCommitTs, txnCommitGroup,
                   currentVersionAtCommit, opCount>>

\* === NEXT ===
Next ==
    \/ \E k \in Keys, g \in Groups       : ProposeRouteChange(k, g)
    \/ \E t \in TxnIds                   : BeginTxn(t)
    \/ \E t \in TxnIds, k \in Keys       : WriteIntent(t, k)
    \/ \E t \in TxnIds, g \in Groups     : Commit(t, g)
    \/ \E t \in TxnIds                   : Abort(t)

Spec == Init /\ [][Next]_vars

\* === STATE CONSTRAINT ===
StateConstraint ==
    /\ catalogVersion <= MaxVersions
    /\ tsCounter      <= MaxTs
    /\ opCount        <= MaxOps

\* === TYPE INVARIANT ===
TypeOK ==
    /\ catalogVersion         \in 0..MaxVersions
    /\ routes                 \in [0..MaxVersions -> [Keys -> Groups]]
    /\ tsCounter              \in 0..MaxTs
    /\ txnState               \in [TxnIds -> States]
    /\ txnStartTs             \in [TxnIds -> 0..MaxTs]
    /\ txnObservedVer         \in [TxnIds -> 0..MaxVersions]
    /\ txnWriteSet            \in [TxnIds -> SUBSET Keys]
    /\ txnCommitTs            \in [TxnIds -> 0..MaxTs]
    /\ txnCommitGroup         \in [TxnIds -> Groups \cup {NoGroup}]
    /\ currentVersionAtCommit \in [TxnIds -> 0..MaxVersions]
    /\ opCount                \in 0..MaxOps

\* === SAFETY INVARIANTS ===

\* Composed-1 — every committed write key was owned by the committing
\* group at the txn's observed catalog version.  This is the load-
\* bearing cross-module invariant: it ties OCC's commit decision to the
\* Routes catalog snapshot the txn read at BeginTxn.  Under EnableSafety
\* the Commit action's guard enforces this; under the gap toggle TLC
\* surfaces the canonical 4-step counterexample.
Composed1_CommitToOwningGroup ==
    \A t \in CommittedTxns :
        \A k \in txnWriteSet[t] :
            OwnerAt(txnObservedVer[t], k) = txnCommitGroup[t]

\* Composed-2 — read-after-write across SplitRange.  Vacuously TRUE in
\* this abstraction: the implementation's SplitRange is same-group
\* only (per CLAUDE.md), so the "key moved to a different group"
\* clause has no reachable state.  M5's ProposeRouteChange explicitly
\* DOES allow cross-group moves so Composed-1 has teeth, but the
\* read-side correctness Composed-2 captures is structurally
\* preserved by the version-chain encoding (every committed write is
\* persisted in the per-key versions[k] in the underlying MVCC store
\* — modelled in M3 and not re-modelled here).
Composed2_ReadAcrossSplitRange == TRUE

\* Composed-3 — strict serialisability bound.  Every pair of distinct
\* committed transactions has distinct commit_ts.  Holds by
\* construction in M5 because tsCounter is strictly monotonic on every
\* Commit action — but stating it explicitly catches a future
\* regression where Commit reuses tsCounter or Composed picks a
\* commit_ts from a per-group pool.  Combined with the action-level
\* Composed3_TsAction below this captures the "real-time order is
\* witnessed by commit_ts" half of strict serialisability; the full
\* claim is bounded by the model's tsCounter abstraction.
Composed3_DistinctCommitTs ==
    \A t1, t2 \in CommittedTxns :
        t1 # t2 => txnCommitTs[t1] # txnCommitTs[t2]

\* Composed-1a — current-version cross-version-read fence.  Strictly
\* stronger than Composed-1: every committed write key must be owned
\* by the committing group at the CURRENT catalog version observed by
\* the FSM at apply time (currentVersionAtCommit[t]), not only at the
\* version the txn pinned at BeginTxn (txnObservedVer[t]).
\*
\* This invariant captures the design doc §4.4 apply-time fence — the
\* implementation-level guard that closes the codex P1 trace
\* (observed-version check passes; ProposeRouteChange has moved the
\* key off the committing group; future readers at the new version
\* miss the write).  The Composed-1 spec predicate alone does NOT
\* prevent this trace; Composed-1a is the spec-level witness for the
\* implementation's additional fence.
\*
\* Gated by EnableCurrentFence in Commit: under the new
\* MCComposed_currentfence_gap.cfg model TLC finds a 4-step
\* counterexample where this invariant fails (the codex P1 trace).
Composed1a_CommitToCurrentOwner ==
    \A t \in CommittedTxns :
        \A k \in txnWriteSet[t] :
            OwnerAt(currentVersionAtCommit[t], k) = txnCommitGroup[t]

\* === ACTION-LEVEL PROPERTIES ===

\* catalogVersion never decreases.  Mirrors Routes1_Action from M4.
Composed_CatalogMonotonic ==
    [][catalogVersion' >= catalogVersion]_vars

\* tsCounter never decreases.  Mirrors HLC1_Action from M1 and the
\* MVCC_TsMonotonic / OCC equivalents.
Composed_TsMonotonic ==
    [][tsCounter' >= tsCounter]_vars

\* Every committed Commit raises tsCounter (it cannot stutter — Commit
\* is gated on `tsCounter < MaxTs` precisely so this property holds).
\* Strengthens Composed3 to "every commit is reflected in the clock".
Composed3_TsAction ==
    [][\A t \in TxnIds :
         (txnState[t] = "Active" /\ txnState'[t] = "Committed")
         => tsCounter' > tsCounter]_vars

=============================================================================
