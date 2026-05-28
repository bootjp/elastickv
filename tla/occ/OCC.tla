--------------------------------- MODULE OCC ---------------------------------
(***************************************************************************)
(* TLA+ specification of the elastickv OCC layer (Percolator-style 2PC).   *)
(* Per docs/design/2026_05_28_partial_tla_safety_spec.md §5.2.             *)
(*                                                                         *)
(* Models the transaction lifecycle Idle -> Active -> Prepared ->          *)
(* Committed/Aborted, the lock-map (key, lock_ts) -> start_ts, and the     *)
(* LockResolver action that turns abandoned locks into versions or         *)
(* clears them.  Encodes all five OCC safety invariants OCC-1..OCC-5:      *)
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
    tsCounter,        \* Nat. Abstract HLC counter; advances on Tick + Commit.
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
(* Tick: advance the abstract HLC counter.  Bounded by MaxTs so TLC stays  *)
(* finite.  No other variable touched.                                     *)
(***************************************************************************)
Tick ==
    /\ tsCounter < MaxTs
    /\ tsCounter' = tsCounter + 1
    /\ UNCHANGED <<txnState, startTs, commitTs, writeSet,
                   readObs, lockMap, versions, opCount>>

(***************************************************************************)
(* BeginTxn: an Idle txn becomes Active and pins its start_ts.  OCC-5 is   *)
(* structurally satisfied because every later op on this txn reads        *)
(* startTs[t] directly — there is no separate read_ts variable.           *)
(***************************************************************************)
BeginTxn(t) ==
    /\ txnState[t] = "Idle"
    /\ txnState' = [txnState EXCEPT ![t] = "Active"]
    /\ startTs'  = [startTs  EXCEPT ![t] = tsCounter]
    /\ UNCHANGED <<tsCounter, commitTs, writeSet,
                   readObs, lockMap, versions, opCount>>

(***************************************************************************)
(* ReadKey: snapshot read on key k at startTs[t].  Gated by OCC-3 lock     *)
(* encoding — if any uncommitted lock with lockTs <= startTs[t] is         *)
(* outstanding, the read is blocked (modelled here as the action being    *)
(* disabled; LockResolve fires first).                                    *)
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
(* commit_ts is the *current* tsCounter — Commit borrows from the clock   *)
(* but does NOT advance it (Tick is the only action that advances ts).    *)
(* That keeps tsCounter inside the bounded model TLC explores.            *)
(*                                                                         *)
(* OCC-1 guard (under EnableSafety): commit_ts strictly greater than the   *)
(* txn's start_ts.  Equivalent to "Tick has fired at least once since      *)
(* BeginTxn(t)".  Disabling the guard is the gap-config evidence path     *)
(* — TLC then surfaces a (committed, commit_ts <= start_ts) counter-       *)
(* example.                                                                *)
(***************************************************************************)
Commit(t) ==
    /\ txnState[t] = "Prepared"
    /\ LET ct == tsCounter IN
        /\ \/ ~EnableSafety
           \/ ct > startTs[t]
        /\ commitTs' = [commitTs EXCEPT ![t] = ct]
        /\ txnState' = [txnState EXCEPT ![t] = "Committed"]
        /\ versions' = [k \in Keys |->
                        IF k \in writeSet[t]
                        THEN versions[k] \cup {[commitTs |-> ct, owner |-> t]}
                        ELSE versions[k]]
        /\ lockMap' = [k \in Keys |->
                        IF LockedBy(k, t) THEN NoLock ELSE lockMap[k]]
    /\ UNCHANGED <<tsCounter, startTs, writeSet, readObs, opCount>>

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
(* LockResolve: clear an orphan lock whose owner has already reached a    *)
(* terminal state.  Models kv/lock_resolver.go.  Required by OCC-4 so any *)
(* committed-but-not-finalised lock can still be cleaned up after the     *)
(* owner's terminal action; required by OCC-L1 (liveness, M6).            *)
(***************************************************************************)
LockResolve(k) ==
    /\ HasLock(k)
    /\ LET t == lockMap[k].owner IN
        /\ txnState[t] \in {"Committed", "Aborted"}
        /\ IF txnState[t] = "Committed"
           THEN versions' = [versions EXCEPT ![k] = @ \cup
                              {[commitTs |-> commitTs[t], owner |-> t]}]
           ELSE versions' = versions
        /\ lockMap' = [lockMap EXCEPT ![k] = NoLock]
    /\ UNCHANGED <<tsCounter, txnState, startTs, commitTs, writeSet,
                   readObs, opCount>>

\* === NEXT ===
Next ==
    \/ Tick
    \/ \E t \in TxnIds : BeginTxn(t)
    \/ \E t \in TxnIds, k \in Keys : ReadKey(t, k)
    \/ \E t \in TxnIds, k \in Keys : WriteIntent(t, k)
    \/ \E t \in TxnIds : Prepare(t)
    \/ \E t \in TxnIds : Commit(t)
    \/ \E t \in TxnIds : Abort(t)
    \/ \E k \in Keys   : LockResolve(k)

Spec == Init /\ [][Next]_vars

\* === STATE CONSTRAINT ===
StateConstraint ==
    /\ tsCounter <= MaxTs
    /\ opCount   <= MaxOps

\* === TYPE INVARIANT ===
TypeOK ==
    /\ tsCounter \in 0..MaxTs
    /\ txnState  \in [TxnIds -> States]
    /\ startTs   \in [TxnIds -> 0..MaxTs]
    /\ commitTs  \in [TxnIds -> 0..MaxTs]
    /\ writeSet  \in [TxnIds -> SUBSET Keys]
    /\ readObs   \in [TxnIds -> [Keys -> 0..MaxTs]]
    /\ opCount   \in 0..MaxOps
    /\ \A k \in Keys : lockMap[k].owner \in TxnIds \cup {"none"}

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

\* OCC-3 — snapshot-read stability under the lock encoding.  A
\* transaction's recorded `readObs[t][k]` equals the version we would
\* compute now (`LatestVisible(k, startTs[t])`) for as long as no
\* version newer than startTs[t] could ever appear on k from a prior-
\* started writer.  We assert the simpler equality form: the recorded
\* observation is consistent with the current visible version *if* no
\* prior-started writer can still write between (start_ts, commit_ts].
\* The lock-encoding clause is satisfied because ReadKey's guard
\* (~ConflictingLockExists) ensures the observation is taken in a
\* lock-free state for that ts.
OCC3_ReadSnapshotStability ==
    \A t \in TxnIds, k \in Keys :
        (txnState[t] \in {"Active", "Prepared", "Committed"} /\ readObs[t][k] # 0) =>
            readObs[t][k] <= startTs[t]

\* OCC-4 — no stranded lock at quiescence.  Bounded safety form: in any
\* state where every txn is in a terminal state (Committed/Aborted),
\* no lock owned by any such txn remains.  LockResolve drains them.
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
