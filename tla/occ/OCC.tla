--------------------------------- MODULE OCC ---------------------------------
(***************************************************************************)
(* TLA+ specification of the elastickv OCC layer.                          *)
(* Per docs/design/2026_05_28_implemented_tla_safety_spec.md §5.2.             *)
(*                                                                         *)
(* Models the transaction paths on main, at the FSM / store level:         *)
(*                                                                         *)
(*   - Snapshot start.  Adapters take start_ts = the store's applied       *)
(*     commit watermark (adapter/ts.go snapshotTS = LastCommitTS); only    *)
(*     these transactions read.  Coordinator-allocated starts             *)
(*     (ShardedCoordinator.nextStartTS) are fresh timestamps and carry no *)
(*     reads.                                                              *)
(*   - One-phase commit (single shard).  The commit_ts is allocated, then *)
(*     the entry is proposed and applied later (Allocate / Apply), so      *)
(*     entries can apply out of timestamp order.  The apply                *)
(*     (kv/fsm.go handleOnePhaseTxnRequest →                               *)
(*     store ApplyMutationsRaftAtFenced) checks: commit_ts > start_ts,     *)
(*     commit_ts above the applied watermark (checkCommitTSFence), no      *)
(*     foreign lock on a write key (assertNoConflictingTxnLocks), and no   *)
(*     version newer than start_ts on any write or read key.               *)
(*   - Two-phase commit (multi shard).  The commit_ts is settled BEFORE   *)
(*     PREPARE (ShardedCoordinator.dispatchTxn → settleTxnCommitTimestamp *)
(*     → prewriteTxn).  PREPARE validates write and read keys against     *)
(*     start_ts and installs write locks only; COMMIT applies at the      *)
(*     settled commit_ts with no watermark fence and no read revalidation *)
(*     (handleCommitRequest → ApplyMutationsRaftAt).                       *)
(*   - Raw writes.  Non-transactional writes (handleRawRequest) take a     *)
(*     fresh timestamp and raise the watermark; they are what lets a      *)
(*     reader's snapshot run ahead of an in-flight 2PC commit_ts.          *)
(*                                                                         *)
(* Invariants OCC-1..OCC-5 are the original contract.  Two more pin known *)
(* gaps from docs/design/2026_09_26_proposed_serializable_isolation_audit *)
(* .md:                                                                    *)
(*                                                                         *)
(*   OCC-3  snapshot-read stability — violated through the 2PC path (G0:   *)
(*          a COMMIT whose commit_ts is below a reader's watermark         *)
(*          snapshot lands after the read).                                *)
(*   OCC-8  no write skew (no rw-antidependency cycle) — violated by a     *)
(*          one-phase commit in a 2PC PREPARE / COMMIT window (G1).        *)
(*                                                                         *)
(* With EnableTwoPhase = FALSE (single-shard transactions only) OCC-1..   *)
(* OCC-5 and OCC-8 all hold.                                               *)
(*                                                                         *)
(* One watermark: the model is one Raft group.  With several groups the   *)
(* one-phase fence compares commit_ts against the applying group's own   *)
(* LastCommitTS, while a snapshot read takes the maximum over all groups  *)
(* (ShardStore.LastCommitTS); outside Phase D nothing validates such a    *)
(* start_ts (validateCrossShardReadTimestamp).  Whether that admits G0 on *)
(* the one-phase path across groups is not checked here.                  *)
(*                                                                         *)
(* EnableSafety gates the commit_ts > start_ts discipline (fresh commit    *)
(* timestamps plus the FSM ErrTxnCommitTSRequired check) so the gap config *)
(* surfaces an OCC-1 counterexample.                                       *)
(*                                                                         *)
(* Timestamps come from a single global counter `tsCounter`: this module  *)
(* assumes unique, leader-issued timestamps (HLC.tla, TSO mode) and does   *)
(* not model timestamp collisions (G10 / G13, see HLC.tla and MVCC.tla).   *)
(* PREPARE is one step per transaction rather than one per shard, and     *)
(* COMMIT drains every lock at once (the asynchronous secondary commit /   *)
(* lock resolver is abstracted away; see the comment above Next).          *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets

CONSTANTS
    Keys,           \* finite set of keys exercised in the model
    TxnIds,         \* finite set of transaction identifiers
    MaxTs,          \* upper bound on the abstract HLC counter (state-space)
    MaxOps,         \* upper bound on Write + RawWrite actions (state-space)
    EnableSafety,   \* TRUE: commit_ts > start_ts discipline; FALSE: gap config
    EnableTwoPhase  \* TRUE: multi-shard 2PC path enabled; FALSE: one-phase only

\* Transaction lifecycle states.
\*   Proposed  — one-phase entry with an allocated commit_ts, not applied yet
\*   Allocated — 2PC commit_ts settled, PREPARE not applied yet
States == {"Idle", "Active", "Proposed", "Allocated", "Prepared",
           "Committed", "Aborted"}

\* Sentinel for "no lock on this key".
NoLock == [owner |-> "none", lockTs |-> 0]

VARIABLES
    tsCounter,        \* Nat. Abstract leader-issued clock; advances on every allocation.
    watermark,        \* Nat. Applied commit watermark (store LastCommitTS).
    txnState,         \* TxnIds -> States
    snapshotRead,     \* TxnIds -> BOOLEAN. TRUE: start_ts = watermark, may read.
    startTs,          \* TxnIds -> Nat   (== read_ts by OCC-5)
    commitTs,         \* TxnIds -> Nat   (0 if not allocated)
    writeSet,         \* TxnIds -> SUBSET Keys
    readSet,          \* TxnIds -> SUBSET Keys
    readObs,          \* TxnIds -> [Keys -> Nat]  (commit_ts observed; 0 = no version)
    lockMap,          \* Keys -> [owner: TxnIds \cup {"none"}, lockTs: Nat]
    versions,         \* Keys -> SUBSET [commitTs: Nat, owner: TxnIds \cup {"raw"}]
    opCount           \* state-space bound

vars == <<tsCounter, watermark, txnState, snapshotRead, startTs, commitTs,
          writeSet, readSet, readObs, lockMap, versions, opCount>>

\* === HELPERS ===
HasLock(k)        == lockMap[k] # NoLock
LockedBy(k, t)    == HasLock(k) /\ lockMap[k].owner = t

\* For OCC-3 lock encoding: a read on k at start_ts is blocked (the
\* implementation returns ErrTxnLocked while the owner is pending)
\* whenever a lock with lockTs <= start_ts exists (cf. §5.2 OCC-3).
ConflictingLockExists(k, ts) ==
    HasLock(k) /\ lockMap[k].lockTs <= ts

\* The committed version visible to a snapshot at read_ts on key k: the
\* largest commitTs <= read_ts among versions[k], 0 if none.
LatestVisible(k, ts) ==
    LET candidates == { v.commitTs : v \in {w \in versions[k] : w.commitTs <= ts} }
    IN IF candidates = {} THEN 0
       ELSE CHOOSE c \in candidates : \A o \in candidates : o <= c

\* A fresh commit_ts.  Under ~EnableSafety the allocation borrows the
\* current counter without advancing, so commit_ts can equal a start_ts
\* allocated by BeginWriteOnly.
FreshCommitTs == IF EnableSafety THEN tsCounter + 1 ELSE tsCounter

\* The FSM's ErrTxnCommitTSRequired check (commit_ts > start_ts),
\* removed together with the fresh allocation in the gap config.
CommitAboveStart(t) == EnableSafety => commitTs[t] > startTs[t]

\* No committed version newer than start_ts on the keys in S
\* (validateConflicts / validateReadConflicts / checkApplyConflicts).
NoNewerVersion(t, S) ==
    \A k \in S : \A v \in versions[k] : v.commitTs <= startTs[t]

\* No other transaction's lock on a write key.
NoForeignLock(t) ==
    \A k \in writeSet[t] : ~HasLock(k) \/ lockMap[k].owner = t

ReleaseLocks(t) ==
    [k \in Keys |-> IF LockedBy(k, t) THEN NoLock ELSE lockMap[k]]

AddVersions(t, ct) ==
    [k \in Keys |->
        IF k \in writeSet[t]
        THEN versions[k] \cup {[commitTs |-> ct, owner |-> t]}
        ELSE versions[k]]

RaiseWatermark(ct) == IF ct > watermark THEN ct ELSE watermark

\* === INIT ===
Init ==
    /\ tsCounter    = 0
    /\ watermark    = 0
    /\ txnState     = [t \in TxnIds |-> "Idle"]
    /\ snapshotRead = [t \in TxnIds |-> FALSE]
    /\ startTs      = [t \in TxnIds |-> 0]
    /\ commitTs     = [t \in TxnIds |-> 0]
    /\ writeSet     = [t \in TxnIds |-> {}]
    /\ readSet      = [t \in TxnIds |-> {}]
    /\ readObs      = [t \in TxnIds |-> [k \in Keys |-> 0]]
    /\ lockMap      = [k \in Keys   |-> NoLock]
    /\ versions     = [k \in Keys   |-> {}]
    /\ opCount      = 0

\* === ACTIONS ===

(***************************************************************************)
(* BeginTxn: an adapter transaction pins start_ts to the applied commit   *)
(* watermark (adapter/ts.go snapshotTS).  No timestamp is allocated.  The *)
(* implementation maps an empty store (watermark 0) to MaxUint64; the     *)
(* model keeps 0, which reads nothing either way.                          *)
(*                                                                         *)
(* OCC-5 is structurally satisfied: every later op on this txn reads     *)
(* startTs[t] directly — there is no separate read_ts variable.          *)
(***************************************************************************)
BeginTxn(t) ==
    /\ txnState[t] = "Idle"
    /\ txnState'     = [txnState     EXCEPT ![t] = "Active"]
    /\ snapshotRead' = [snapshotRead EXCEPT ![t] = TRUE]
    /\ startTs'      = [startTs      EXCEPT ![t] = watermark]
    /\ UNCHANGED <<tsCounter, watermark, commitTs, writeSet, readSet,
                   readObs, lockMap, versions, opCount>>

(***************************************************************************)
(* BeginWriteOnly: a coordinator-stamped transaction (no caller start_ts) *)
(* allocates a fresh start_ts above the touched keys' latest commits      *)
(* (ShardedCoordinator.nextStartTS).  It performed no reads, so ReadKey   *)
(* is disabled for it.                                                     *)
(***************************************************************************)
BeginWriteOnly(t) ==
    /\ txnState[t] = "Idle"
    /\ tsCounter < MaxTs
    /\ tsCounter'    = tsCounter + 1
    /\ txnState'     = [txnState     EXCEPT ![t] = "Active"]
    /\ snapshotRead' = [snapshotRead EXCEPT ![t] = FALSE]
    /\ startTs'      = [startTs      EXCEPT ![t] = tsCounter + 1]
    /\ UNCHANGED <<watermark, commitTs, writeSet, readSet, readObs,
                   lockMap, versions, opCount>>

(***************************************************************************)
(* ReadKey: snapshot read on key k at startTs[t].  Blocked while a lock   *)
(* with lockTs <= startTs[t] is outstanding (modelled as the action being *)
(* disabled until the lock holder's Commit or Abort drains the lock).     *)
(***************************************************************************)
ReadKey(t, k) ==
    /\ txnState[t] = "Active"
    /\ snapshotRead[t]
    /\ k \notin readSet[t]
    /\ ~ConflictingLockExists(k, startTs[t])
    /\ readSet' = [readSet EXCEPT ![t] = @ \cup {k}]
    /\ readObs' = [readObs EXCEPT ![t] = [@ EXCEPT ![k] = LatestVisible(k, startTs[t])]]
    /\ UNCHANGED <<tsCounter, watermark, txnState, snapshotRead, startTs,
                   commitTs, writeSet, lockMap, versions, opCount>>

(***************************************************************************)
(* WriteIntent: buffer a write to k locally in the txn's writeSet.        *)
(***************************************************************************)
WriteIntent(t, k) ==
    /\ txnState[t] = "Active"
    /\ k \notin writeSet[t]
    /\ opCount < MaxOps
    /\ writeSet' = [writeSet EXCEPT ![t] = @ \cup {k}]
    /\ opCount'  = opCount + 1
    /\ UNCHANGED <<tsCounter, watermark, txnState, snapshotRead, startTs,
                   commitTs, readSet, readObs, lockMap, versions>>

(***************************************************************************)
(* RawWrite(k): a non-transactional write (handleRawRequest).  Allocation *)
(* and apply are one step here, so the raw path's own reordering window   *)
(* is not modelled; its role is to raise the watermark.                    *)
(***************************************************************************)
RawWrite(k) ==
    /\ opCount < MaxOps
    /\ tsCounter < MaxTs
    /\ LET ct == tsCounter + 1 IN
        /\ tsCounter' = ct
        /\ versions'  = [versions EXCEPT ![k] = @ \cup {[commitTs |-> ct, owner |-> "raw"]}]
        /\ watermark' = RaiseWatermark(ct)
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<txnState, snapshotRead, startTs, commitTs, writeSet,
                   readSet, readObs, lockMap>>

\* --- One-phase path ---------------------------------------------------

(***************************************************************************)
(* AllocateOnePhase: the coordinator allocates commit_ts and proposes the *)
(* one-phase entry.  Nothing is applied yet.                               *)
(***************************************************************************)
AllocateOnePhase(t) ==
    /\ txnState[t] = "Active"
    /\ writeSet[t] # {}
    /\ EnableSafety => tsCounter < MaxTs
    /\ tsCounter' = IF EnableSafety THEN tsCounter + 1 ELSE tsCounter
    /\ commitTs'  = [commitTs EXCEPT ![t] = FreshCommitTs]
    /\ txnState'  = [txnState EXCEPT ![t] = "Proposed"]
    /\ UNCHANGED <<watermark, snapshotRead, startTs, writeSet, readSet,
                   readObs, lockMap, versions, opCount>>

\* Every check the one-phase apply performs.
OnePhaseApplies(t) ==
    /\ CommitAboveStart(t)
    /\ commitTs[t] > watermark                  \* checkCommitTSFence
    /\ NoForeignLock(t)                          \* assertNoConflictingTxnLocks
    /\ NoNewerVersion(t, writeSet[t])
    /\ NoNewerVersion(t, readSet[t])

(***************************************************************************)
(* ApplyOnePhase / RejectOnePhase: the FSM applies the proposed entry, in *)
(* any order relative to other proposed entries, or deterministically     *)
(* rejects it.                                                             *)
(***************************************************************************)
ApplyOnePhase(t) ==
    /\ txnState[t] = "Proposed"
    /\ OnePhaseApplies(t)
    /\ txnState'  = [txnState EXCEPT ![t] = "Committed"]
    /\ versions'  = AddVersions(t, commitTs[t])
    /\ watermark' = RaiseWatermark(commitTs[t])
    /\ UNCHANGED <<tsCounter, snapshotRead, startTs, commitTs, writeSet,
                   readSet, readObs, lockMap, opCount>>

RejectOnePhase(t) ==
    /\ txnState[t] = "Proposed"
    /\ ~OnePhaseApplies(t)
    /\ txnState' = [txnState EXCEPT ![t] = "Aborted"]
    /\ UNCHANGED <<tsCounter, watermark, snapshotRead, startTs, commitTs,
                   writeSet, readSet, readObs, lockMap, versions, opCount>>

\* --- Two-phase path ---------------------------------------------------

(***************************************************************************)
(* AllocateTwoPhase: settleTxnCommitTimestamp fixes commit_ts before any  *)
(* PREPARE is proposed.                                                    *)
(***************************************************************************)
AllocateTwoPhase(t) ==
    /\ EnableTwoPhase
    /\ txnState[t] = "Active"
    /\ writeSet[t] # {}
    /\ EnableSafety => tsCounter < MaxTs
    /\ tsCounter' = IF EnableSafety THEN tsCounter + 1 ELSE tsCounter
    /\ commitTs'  = [commitTs EXCEPT ![t] = FreshCommitTs]
    /\ txnState'  = [txnState EXCEPT ![t] = "Allocated"]
    /\ UNCHANGED <<watermark, snapshotRead, startTs, writeSet, readSet,
                   readObs, lockMap, versions, opCount>>

(***************************************************************************)
(* Prepare: handlePrepareRequest validates the write and read keys        *)
(* against start_ts and installs write locks at lockTs = start_ts.  Read  *)
(* keys get no lock.  This is the OCC-2 enforcement point.                *)
(***************************************************************************)
Prepare(t) ==
    /\ txnState[t] = "Allocated"
    /\ NoForeignLock(t)
    /\ NoNewerVersion(t, writeSet[t])
    /\ NoNewerVersion(t, readSet[t])
    /\ txnState' = [txnState EXCEPT ![t] = "Prepared"]
    /\ lockMap' = [k \in Keys |->
                    IF k \in writeSet[t]
                    THEN [owner |-> t, lockTs |-> startTs[t]]
                    ELSE lockMap[k]]
    /\ UNCHANGED <<tsCounter, watermark, snapshotRead, startTs, commitTs,
                   writeSet, readSet, readObs, versions, opCount>>

(***************************************************************************)
(* Commit: handleCommitRequest promotes every lock owned by t into a      *)
(* version at the settled commit_ts.  No watermark fence, no read         *)
(* revalidation.                                                           *)
(***************************************************************************)
Commit(t) ==
    /\ txnState[t] = "Prepared"
    /\ CommitAboveStart(t)
    /\ txnState'  = [txnState EXCEPT ![t] = "Committed"]
    /\ versions'  = AddVersions(t, commitTs[t])
    /\ lockMap'   = ReleaseLocks(t)
    /\ watermark' = RaiseWatermark(commitTs[t])
    /\ UNCHANGED <<tsCounter, snapshotRead, startTs, commitTs, writeSet,
                   readSet, readObs, opCount>>

(***************************************************************************)
(* Abort: the client gives up before proposing, or the coordinator / lock *)
(* resolver rolls back a 2PC transaction.  Releases all locks of t.       *)
(* A proposed one-phase entry is settled by the FSM, not by Abort.        *)
(***************************************************************************)
Abort(t) ==
    /\ txnState[t] \in {"Active", "Allocated", "Prepared"}
    /\ txnState' = [txnState EXCEPT ![t] = "Aborted"]
    /\ lockMap'  = ReleaseLocks(t)
    /\ UNCHANGED <<tsCounter, watermark, snapshotRead, startTs, commitTs,
                   writeSet, readSet, readObs, versions, opCount>>

(***************************************************************************)
(* LockResolve is intentionally NOT a separate action.                     *)
(*                                                                         *)
(* The real implementation commits the primary first and resolves         *)
(* secondary locks asynchronously (kv/lock_resolver.go, ShardStore        *)
(* maybeResolveTxnLock): a reader that meets a lock whose primary is      *)
(* committed or rolled back resolves it.  Commit and Abort above drain    *)
(* every lock in the same step, which subsumes the resolver's effect on   *)
(* the durable state without modelling the interleaving.  Known flaws in  *)
(* that interleaving (audit G12, G14) are therefore out of scope here.    *)
(***************************************************************************)

\* === NEXT ===
Next ==
    \/ \E t \in TxnIds : BeginTxn(t)
    \/ \E t \in TxnIds : BeginWriteOnly(t)
    \/ \E t \in TxnIds, k \in Keys : ReadKey(t, k)
    \/ \E t \in TxnIds, k \in Keys : WriteIntent(t, k)
    \/ \E k \in Keys : RawWrite(k)
    \/ \E t \in TxnIds : AllocateOnePhase(t)
    \/ \E t \in TxnIds : ApplyOnePhase(t)
    \/ \E t \in TxnIds : RejectOnePhase(t)
    \/ \E t \in TxnIds : AllocateTwoPhase(t)
    \/ \E t \in TxnIds : Prepare(t)
    \/ \E t \in TxnIds : Commit(t)
    \/ \E t \in TxnIds : Abort(t)

Spec == Init /\ [][Next]_vars

\* SpecLive is Spec with the M6 fairness assumptions that drive the
\* OCC-L1 liveness property: weak fairness on Abort(t) (enabled in
\* Active / Allocated / Prepared) and on the FSM settling a proposed
\* one-phase entry (exactly one of ApplyOnePhase / RejectOnePhase is
\* enabled in Proposed).  Together they make every non-Idle txn reach
\* Committed or Aborted.
\*
\* Kept as a separate definition (not folded into Spec) so the safety
\* runs remain pure safety checks with no fairness assumptions; only
\* MCOCC_live.cfg points at SpecLive.
SpecLive ==
    /\ Init
    /\ [][Next]_vars
    /\ \A t \in TxnIds : WF_vars(Abort(t))
    /\ \A t \in TxnIds : WF_vars(ApplyOnePhase(t) \/ RejectOnePhase(t))

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
    /\ tsCounter    \in 0..MaxTs
    /\ watermark    \in 0..MaxTs
    /\ txnState     \in [TxnIds -> States]
    /\ snapshotRead \in [TxnIds -> BOOLEAN]
    /\ startTs      \in [TxnIds -> 0..MaxTs]
    /\ commitTs     \in [TxnIds -> 0..MaxTs]
    /\ writeSet     \in [TxnIds -> SUBSET Keys]
    /\ readSet      \in [TxnIds -> SUBSET Keys]
    /\ readObs      \in [TxnIds -> [Keys -> 0..MaxTs]]
    /\ opCount      \in 0..MaxOps
    /\ \A k \in Keys :
        /\ lockMap[k].owner  \in TxnIds \cup {"none"}
        /\ lockMap[k].lockTs \in 0..MaxTs
    /\ versions \in [Keys -> SUBSET [commitTs: 0..MaxTs, owner: TxnIds \cup {"raw"}]]

\* Set of committed transactions.
CommittedTxns == { t \in TxnIds : txnState[t] = "Committed" }

\* Transactions whose reads still matter (aborted ones are discarded).
LiveTxns == { t \in TxnIds : txnState[t] \notin {"Idle", "Aborted"} }

\* === SAFETY INVARIANTS ===

\* OCC-1 — every committed transaction has commit_ts strictly greater
\* than its start_ts.  Holds under EnableSafety; removed in the gap
\* config so TLC produces the motivating counterexample.
OCC1_CommitTsAboveStart ==
    \A t \in CommittedTxns : commitTs[t] > startTs[t]

\* OCC-2 — no write-write conflict on intersecting write sets.  When two
\* committed transactions share a write key, (a) their commit
\* timestamps are distinct and (b) the later starter's snapshot covers
\* the earlier one's commit (`commitTs[earlier] <= startTs[later]`).
\* This is a timestamp-order statement; whether the later transaction
\* actually SAW the earlier write is OCC-3's business (G0 breaks that
\* without breaking OCC-2).
OCC2_NoWriteWriteConflict ==
    \A t1, t2 \in CommittedTxns :
        (t1 # t2 /\ writeSet[t1] \cap writeSet[t2] # {}) =>
            /\ commitTs[t1] # commitTs[t2]
            /\ (commitTs[t1] <= startTs[t2] \/ commitTs[t2] <= startTs[t1])

\* OCC-3 — snapshot-read stability.  What a live transaction read at
\* (k, start_ts) is still the latest visible version there.  Violated
\* on main through the 2PC path (audit G0): COMMIT applies a commit_ts
\* settled before PREPARE, with no watermark fence, below the snapshot
\* of a reader that already read the key.
OCC3_ReadSnapshotStability ==
    \A t \in LiveTxns, k \in Keys :
        k \in readSet[t] => readObs[t][k] = LatestVisible(k, startTs[t])

\* OCC-4 — no stranded lock at quiescence.  Bounded safety form: in any
\* state where every txn is Idle / Committed / Aborted, no lock remains.
OCC4_NoStrandedLockAtQuiescence ==
    LET AllTerminal == \A t \in TxnIds : txnState[t] \in {"Idle", "Committed", "Aborted"}
    IN AllTerminal =>
        \A k \in Keys : ~HasLock(k)

\* OCC-5 — start_ts equals read_ts for every operation.  Structurally
\* satisfied by the model (no separate read_ts variable; every read uses
\* startTs[t] directly).  Asserted as a sanity check: every read
\* observation is bounded by the txn's startTs.
OCC5_StartTsConsistency ==
    \A t \in TxnIds, k \in Keys :
        k \in readSet[t] => readObs[t][k] <= startTs[t]

\* OCC-8 — no write skew.  t1 -rw-> t2 when t1 read a key t2 wrote and
\* t2's write is not in t1's snapshot (commitTs[t2] > startTs[t1]).  Two
\* committed transactions with rw edges in both directions form a cycle
\* no serial order explains.  Violated on main (audit G1): 2PC read keys
\* are validated once at PREPARE and not protected until COMMIT.
RW(t1, t2) ==
    \E k \in readSet[t1] \cap writeSet[t2] : commitTs[t2] > startTs[t1]

OCC8_NoWriteSkew ==
    \A t1, t2 \in CommittedTxns :
        t1 # t2 => ~(RW(t1, t2) /\ RW(t2, t1))

\* === ACTION-LEVEL INVARIANTS (registered under PROPERTIES) ===

\* Each txn's startTs is assigned once (at BeginTxn / BeginWriteOnly)
\* and never updated.
OCC5_Action ==
    [][\A t \in TxnIds : txnState[t] # "Idle" => startTs'[t] = startTs[t]]_vars

\* commitTs is assigned once (at allocation) and never updated.
CommitTsAssignedOnce ==
    [][\A t \in TxnIds : commitTs[t] # 0 => commitTs'[t] = commitTs[t]]_vars

\* === LIVENESS PROPERTIES (M6) ===

\* OCC-L1 — every txn eventually settles into a state where it is
\* either Idle (never started) or terminal (Committed/Aborted), and
\* it stays there.  Prevents the "stranded txn" failure mode where a
\* prepared write holds a lock indefinitely because the coordinator
\* never picks up the abort path.
\*
\* Only checked when the harness points at SpecLive (MCOCC_live.cfg).
\* The `<>[]` form ("eventually always") matches TLC's preferred
\* shape for terminating reactive systems.
OCC_L1_TxnTerminates ==
    <>[]( \A t \in TxnIds :
            txnState[t] \in {"Idle", "Committed", "Aborted"} )

=============================================================================
