--------------------------------- MODULE OCC ---------------------------------
(***************************************************************************)
(* TLA+ specification of the elastickv OCC layer (Percolator-style 2PC).   *)
(* Per docs/design/2026_05_28_partial_tla_safety_spec.md §5.2.             *)
(*                                                                         *)
(* Models the transaction lifecycle Idle -> Active -> Prepared ->          *)
(* Committed/Aborted and the lock-map (key, lock_ts) -> start_ts.  In M2  *)
(* the lock-resolver (kv/lock_resolver.go) is abstracted into the atomic  *)
(* lock drain inside Commit/Abort; see the block comment further down for *)
(* why a separate LockResolve action is unreachable here and deferred to  *)
(* M5 (composed).  Encodes all five OCC safety invariants OCC-1..OCC-5:    *)
(*                                                                         *)
(*   OCC-1  Commit_ts > read_ts for every committed transaction.           *)
(*   OCC-2  No write-write conflict on intersecting write sets.            *)
(*   OCC-3  Snapshot read stability under the lock-encoding clause.        *)
(*   OCC-4  No stranded lock at quiescence (bounded-safety form).          *)
(*   OCC-5  start_ts = read_ts for every operation in the same txn.        *)
(*                                                                         *)
(* The single CONSTANT EnableSafety gates two normative guards so the      *)
(* same module drives the safe model-check (MCOCC.cfg) and the gap         *)
(* model-check (MCOCC_gap.cfg) — when off, the Commit action skips the    *)
(* OCC-1 strict-greater check, which TLC surfaces as an explicit           *)
(* counterexample (the design doc's motivating evidence pattern).          *)
(*                                                                         *)
(* HLC is abstracted to a single global monotonic counter `tsCounter`;     *)
(* M5 (composed) will INSTANCE HLC.tla for the real 48/16 layout.          *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
    Keys,           \* finite set of keys exercised in the model
    TxnIds,         \* finite set of transaction identifiers
    MaxTs,          \* upper bound on the abstract HLC counter (state-space)
    MaxOps,         \* upper bound on per-txn Write actions (state-space)
    EnableSafety    \* TRUE: encode OCC-1 commit guard; FALSE: gap config

\* Transaction lifecycle states.
States == {"Idle", "Active", "Prepared", "Committed", "Aborted"}

\* Sentinel for "no lock on this key".
NoLock == [owner |-> "none", lockTs |-> 0]

VARIABLES
    tsCounter,        \* Nat. Abstract HLC counter; advances on BeginTxn + Commit.
    txnState,         \* TxnIds -> States
    startTs,          \* TxnIds -> Nat   (== read_ts by OCC-5)
    commitTs,         \* TxnIds -> Nat   (0 if not committed)
    writeSet,         \* TxnIds -> SUBSET Keys
    readObs,          \* TxnIds -> [Keys -> Nat]  (commit_ts observed; 0 = nothing)
    lockMap,          \* Keys -> [owner: TxnIds \cup {"none"}, lockTs: Nat]
    versions,         \* Keys -> SUBSET [commitTs: Nat, owner: TxnIds]
    opCount           \* state-space bound

vars == <<tsCounter, txnState, startTs, commitTs, writeSet,
          readObs, lockMap, versions, opCount>>

\* === HELPERS ===
HasLock(k)        == lockMap[k] # NoLock
LockedBy(k, t)    == HasLock(k) /\ lockMap[k].owner = t

\* For OCC-3 lock encoding: a read on k at start_ts must skip the version
\* read whenever a lock with lockTs <= start_ts exists (cf. §5.2 OCC-3).
ConflictingLockExists(k, ts) ==
    HasLock(k) /\ lockMap[k].lockTs <= ts

\* The committed version visible to a snapshot at read_ts on key k: the
\* largest commitTs strictly <= read_ts among versions[k], 0 if none.
LatestVisible(k, ts) ==
    LET candidates == { v.commitTs : v \in {w \in versions[k] : w.commitTs <= ts} }
    IN IF candidates = {} THEN 0
       ELSE CHOOSE c \in candidates : \A o \in candidates : o <= c

\* === INIT ===
Init ==
    /\ tsCounter = 0
    /\ txnState  = [t \in TxnIds |-> "Idle"]
    /\ startTs   = [t \in TxnIds |-> 0]
    /\ commitTs  = [t \in TxnIds |-> 0]
    /\ writeSet  = [t \in TxnIds |-> {}]
    /\ readObs   = [t \in TxnIds |-> [k \in Keys |-> 0]]
    /\ lockMap   = [k \in Keys   |-> NoLock]
    /\ versions  = [k \in Keys   |-> {}]
    /\ opCount   = 0

\* === ACTIONS ===

(***************************************************************************)
(* BeginTxn: an Idle txn becomes Active and pins its start_ts.            *)
(* tsCounter is advanced as part of the action so the assigned startTs    *)
(* is a fresh, monotonically increasing value across all BeginTxn calls.  *)
(* This models the real HLC behaviour where start_ts is allocated from a  *)
(* leader-issued HLC ts that strictly increases per allocation.           *)
(*                                                                         *)
(* The monotonic-tsCounter property is load-bearing for OCC-3 (snapshot   *)
(* stability — strict equality form): a reader's `LatestVisible(k, T)`    *)
(* cannot change after the read because no future Commit can produce a    *)
(* commit_ts <= T (Commit also auto-advances tsCounter under              *)
(* EnableSafety; see Commit below).                                       *)
(*                                                                         *)
(* OCC-5 is structurally satisfied: every later op on this txn reads     *)
(* startTs[t] directly — there is no separate read_ts variable.          *)
(***************************************************************************)
BeginTxn(t) ==
    /\ txnState[t] = "Idle"
    /\ tsCounter < MaxTs
    /\ tsCounter' = tsCounter + 1
    /\ txnState' = [txnState EXCEPT ![t] = "Active"]
    /\ startTs'  = [startTs  EXCEPT ![t] = tsCounter + 1]
    /\ UNCHANGED <<commitTs, writeSet, readObs, lockMap, versions, opCount>>

(***************************************************************************)
(* ReadKey: snapshot read on key k at startTs[t].  Gated by OCC-3 lock     *)
(* encoding — if any uncommitted lock with lockTs <= startTs[t] is         *)
(* outstanding, the read is blocked (modelled here as the action being    *)
(* disabled until the lock holder's Commit or Abort drains the lock in    *)
(* that same atomic step).                                                 *)
(***************************************************************************)
ReadKey(t, k) ==
    /\ txnState[t] = "Active"
    /\ readObs[t][k] = 0                  \* not yet read in this txn
    /\ ~ConflictingLockExists(k, startTs[t])
    /\ readObs' = [readObs EXCEPT ![t] = [@ EXCEPT ![k] = LatestVisible(k, startTs[t])]]
    /\ UNCHANGED <<tsCounter, txnState, startTs, commitTs, writeSet,
                   lockMap, versions, opCount>>

(***************************************************************************)
(* WriteIntent: buffer a write to k locally in the txn's writeSet.        *)
(* No lock is taken yet (Percolator pre-prepare).                          *)
(***************************************************************************)
WriteIntent(t, k) ==
    /\ txnState[t] = "Active"
    /\ k \notin writeSet[t]
    /\ opCount < MaxOps
    /\ writeSet' = [writeSet EXCEPT ![t] = @ \cup {k}]
    /\ opCount'  = opCount + 1
    /\ UNCHANGED <<tsCounter, txnState, startTs, commitTs,
                   readObs, lockMap, versions>>

(***************************************************************************)
(* Prepare: acquire locks on every key in writeSet at lockTs = startTs[t]. *)
(* The conflict check (no existing lock and no version with               *)
(* commitTs > startTs[t]) prevents two concurrent writers from prepar-    *)
(* ing on the same key — this is the OCC-2 enforcement point.             *)
(***************************************************************************)
Prepare(t) ==
    /\ txnState[t] = "Active"
    /\ writeSet[t] # {}
    /\ \A k \in writeSet[t] :
        /\ ~HasLock(k)
        /\ \A v \in versions[k] : v.commitTs <= startTs[t]
    /\ txnState' = [txnState EXCEPT ![t] = "Prepared"]
    /\ lockMap' = [k \in Keys |->
                    IF k \in writeSet[t]
                    THEN [owner |-> t, lockTs |-> startTs[t]]
                    ELSE lockMap[k]]
    /\ UNCHANGED <<tsCounter, startTs, commitTs, writeSet,
                   readObs, versions, opCount>>

(***************************************************************************)
(* Commit: promote every lock owned by t into a version at commitTs.       *)
(*                                                                         *)
(* Under EnableSafety, Commit advances tsCounter and uses the new value    *)
(* as commit_ts — this models the HLC behaviour where commit_ts is        *)
(* allocated as the next fresh ts > all prior start_ts and commit_ts.     *)
(* The OCC-1 invariant (`commit_ts > start_ts`) is therefore a consequence *)
(* of the monotonic ts allocation rather than an explicit comparison.     *)
(*                                                                         *)
(* Under ~EnableSafety (gap config), Commit borrows the *current*         *)
(* tsCounter without advancing.  Combined with BeginTxn's auto-advance,   *)
(* this lets commit_ts == startTs[t] (the BeginTxn's freshly assigned     *)
(* value), violating OCC-1.  TLC surfaces the (committed, commit_ts ==    *)
(* start_ts) counterexample.                                              *)
(***************************************************************************)
Commit(t) ==
    /\ txnState[t] = "Prepared"
    /\ LET ct == IF EnableSafety THEN tsCounter + 1 ELSE tsCounter IN
        /\ EnableSafety => tsCounter < MaxTs   \* keep ct inside the bounded model
        /\ tsCounter' = IF EnableSafety THEN tsCounter + 1 ELSE tsCounter
        /\ commitTs' = [commitTs EXCEPT ![t] = ct]
        /\ txnState' = [txnState EXCEPT ![t] = "Committed"]
        /\ versions' = [k \in Keys |->
                        IF k \in writeSet[t]
                        THEN versions[k] \cup {[commitTs |-> ct, owner |-> t]}
                        ELSE versions[k]]
        /\ lockMap' = [k \in Keys |->
                        IF LockedBy(k, t) THEN NoLock ELSE lockMap[k]]
    /\ UNCHANGED <<startTs, writeSet, readObs, opCount>>

(***************************************************************************)
(* Abort: release all locks owned by t.  Versions are NOT created.        *)
(***************************************************************************)
Abort(t) ==
    /\ txnState[t] \in {"Active", "Prepared"}
    /\ txnState' = [txnState EXCEPT ![t] = "Aborted"]
    /\ lockMap'  = [k \in Keys |->
                    IF LockedBy(k, t) THEN NoLock ELSE lockMap[k]]
    /\ UNCHANGED <<tsCounter, startTs, commitTs, writeSet,
                   readObs, versions, opCount>>

(***************************************************************************)
(* LockResolve is intentionally NOT part of this M2 model.                 *)
(*                                                                         *)
(* The real implementation has an asynchronous lock-resolver               *)
(* (kv/lock_resolver.go): when a reader encounters a lock whose owner is  *)
(* in a terminal state, the resolver either promotes the lock to a        *)
(* version (Committed owner) or clears it (Aborted owner).  Modelling      *)
(* that resolver as a TLA+ action only makes sense if Commit/Abort can    *)
(* leave orphan locks behind — which in our `Commit`/`Abort` above is     *)
(* not the case: both actions atomically drain every lock they own as    *)
(* part of the same step.  A LockResolve action whose guard requires      *)
(* HasLock(k) /\ txnState[owner] \in {Committed, Aborted} would           *)
(* therefore be unreachable in this model (gemini HIGH on PR #858).      *)
(*                                                                         *)
(* This is a deliberate M2 abstraction: the atomic-drain in Commit/Abort  *)
(* subsumes the resolver's effect on the durable state without modelling  *)
(* the asynchronous interleaving.  The bounded-safety form of OCC-4 (no   *)
(* stranded lock at quiescence) is therefore trivially preserved.         *)
(* OCC-L1 (eventual lock release, the liveness counterpart) is deferred  *)
(* to M6 per §5.6 of the design doc.  M5 (composed) is the right place    *)
(* to model the async resolver: cross-module interactions with OCC-3      *)
(* (lock encoding for reads) and Routes (where the resolver issues        *)
(* secondary RPCs) make sense to test together.                            *)
(***************************************************************************)

\* === NEXT ===
Next ==
    \/ \E t \in TxnIds : BeginTxn(t)
    \/ \E t \in TxnIds, k \in Keys : ReadKey(t, k)
    \/ \E t \in TxnIds, k \in Keys : WriteIntent(t, k)
    \/ \E t \in TxnIds : Prepare(t)
    \/ \E t \in TxnIds : Commit(t)
    \/ \E t \in TxnIds : Abort(t)

Spec == Init /\ [][Next]_vars

\* === STATE CONSTRAINT ===
StateConstraint ==
    /\ tsCounter <= MaxTs
    /\ opCount   <= MaxOps

\* === TYPE INVARIANT ===
\* Every state variable is exhaustively typed (gemini medium on PR
\* #858).  Without coverage of `lockMap[k].lockTs` and `versions`,
\* TypeOK would let a buggy action smuggle in a malformed lock-ts or
\* version record undetected.
TypeOK ==
    /\ tsCounter \in 0..MaxTs
    /\ txnState  \in [TxnIds -> States]
    /\ startTs   \in [TxnIds -> 0..MaxTs]
    /\ commitTs  \in [TxnIds -> 0..MaxTs]
    /\ writeSet  \in [TxnIds -> SUBSET Keys]
    /\ readObs   \in [TxnIds -> [Keys -> 0..MaxTs]]
    /\ opCount   \in 0..MaxOps
    /\ \A k \in Keys :
        /\ lockMap[k].owner  \in TxnIds \cup {"none"}
        /\ lockMap[k].lockTs \in 0..MaxTs
    /\ versions \in [Keys -> SUBSET [commitTs: 0..MaxTs, owner: TxnIds]]

\* Set of transactions in a terminal state.
CommittedTxns == { t \in TxnIds : txnState[t] = "Committed" }

\* === SAFETY INVARIANTS ===

\* OCC-1 — every committed transaction has commit_ts strictly greater
\* than its start_ts.  Enforced at the Commit action under EnableSafety;
\* removed in the gap config so TLC produces the motivating counter-
\* example.
OCC1_CommitTsAboveStart ==
    \A t \in CommittedTxns : commitTs[t] > startTs[t]

\* OCC-2 — no write-write conflict on intersecting write sets.  When two
\* committed transactions share a write key, (a) their commit
\* timestamps are distinct and (b) the later starter saw the earlier
\* one (`commitTs[earlier] <= startTs[later]`).  The non-strict
\* `<=` is correct: a transaction with `start_ts = T` sees every
\* version with `commit_ts <= T`, so `start_ts = commit_ts[earlier]`
\* still counts as "the later one ran in the world after the earlier
\* one committed".
OCC2_NoWriteWriteConflict ==
    \A t1, t2 \in CommittedTxns :
        (t1 # t2 /\ writeSet[t1] \cap writeSet[t2] # {}) =>
            /\ commitTs[t1] # commitTs[t2]
            /\ (commitTs[t1] <= startTs[t2] \/ commitTs[t2] <= startTs[t1])

\* OCC-3 — snapshot-read stability under the lock encoding.  The
\* recorded `readObs[t][k]` equals the current `LatestVisible(k,
\* startTs[t])` for every key the transaction has read.  The previous
\* weaker `<=` form (gemini medium on PR #858) only asserted bounds
\* and overlapped with OCC-5; this strict equality is the real
\* snapshot-stability claim — the read value MUST remain consistent
\* with the canonical latest-visible version at the txn's snapshot.
\*
\* Soundness in the safe config rests on the monotonic ts allocation
\* in BeginTxn/Commit: every BeginTxn issues a fresh startTs >
\* tsCounter, and every Commit issues a commit_ts > tsCounter > every
\* prior startTs.  So no future Commit can produce a commit_ts <=
\* startTs[t] for any t already past BeginTxn, hence LatestVisible(k,
\* startTs[t]) cannot change after the read.
\*
\* The `readObs[t][k] # 0` precondition conflates "did not read" and
\* "read but observed no version"; this is acceptable because the
\* latter case is vacuously stable (no version <= startTs[t] either
\* before or after, under the monotonic-ts allocation).
OCC3_ReadSnapshotStability ==
    \A t \in TxnIds, k \in Keys :
        (txnState[t] \in {"Active", "Prepared", "Committed"} /\ readObs[t][k] # 0) =>
            readObs[t][k] = LatestVisible(k, startTs[t])

\* OCC-4 — no stranded lock at quiescence.  Bounded safety form: in any
\* state where every txn is in a terminal state (Idle/Committed/Aborted),
\* no lock remains on any key.  Under the M2 atomic-drain abstraction
\* (Commit/Abort clear every lock they own in the same step), this is
\* maintained by construction rather than by a separate resolver action.
\* The async resolver case is deferred to M5 — see the block comment
\* above where LockResolve would have lived.
OCC4_NoStrandedLockAtQuiescence ==
    LET AllTerminal == \A t \in TxnIds : txnState[t] \in {"Idle", "Committed", "Aborted"}
    IN AllTerminal =>
        \A k \in Keys : ~HasLock(k)

\* OCC-5 — start_ts equals read_ts for every operation.  Structurally
\* satisfied by the model (no separate read_ts variable; every read uses
\* startTs[t] directly).  Asserted here as a sanity check on the
\* invariant: every read observation is bounded by the txn's startTs.
\* (OCC-3 already implies this; OCC-5 is the named invariant.)
OCC5_StartTsConsistency ==
    \A t \in TxnIds, k \in Keys :
        readObs[t][k] # 0 => readObs[t][k] <= startTs[t]

\* === ACTION-LEVEL INVARIANTS (registered under PROPERTIES) ===

\* Each txn's startTs is assigned once (at BeginTxn) and never updated.
OCC5_Action ==
    [][\A t \in TxnIds : startTs[t] # 0 => startTs'[t] = startTs[t]]_vars

\* commitTs is assigned once (at Commit) and never updated.
CommitTsAssignedOnce ==
    [][\A t \in TxnIds : commitTs[t] # 0 => commitTs'[t] = commitTs[t]]_vars

=============================================================================
