--------------------------------- MODULE MVCC --------------------------------
(***************************************************************************)
(* TLA+ specification of the elastickv MVCC layer.                         *)
(* Per docs/design/2026_05_28_implemented_tla_safety_spec.md §5.3.             *)
(*                                                                         *)
(* Models a per-key version chain `versions[k]` (store/lsm_store.go) and   *)
(* the store's resumable compaction:                                       *)
(*                                                                         *)
(*   - BeginCompact persists the pending watermark first                   *)
(*     (beginCompaction → setPendingMinRetainedTSLocked), so reads below   *)
(*     it fail closed (effectiveMinRetainedTS) before anything is deleted. *)
(*   - PruneKey deletes one key's garbage (scanCompactionDeletes).  Delete *)
(*     batches commit independently and a crash re-runs the scan, so the   *)
(*     step is idempotent and keys are pruned in any order.                *)
(*   - FinishCompact commits the watermark once every key is pruned        *)
(*     (commitCompactionMinRetainedTS).                                    *)
(*                                                                         *)
(* Per key the prune keeps every version with commit_ts > minTS plus the   *)
(* newest version with commit_ts <= minTS (shouldDeleteCompactionVersion). *)
(* Reads below the effective watermark return ErrReadTSCompacted          *)
(* (readTSCompacted), so MVCC-4 only constrains read_ts >= that watermark. *)
(*                                                                         *)
(* Writes take a fresh timestamp.  With UniqueTs = FALSE the model also    *)
(* admits a write whose timestamp collides with an existing version of the *)
(* key, which is what non-leader stamping produces (audit G10; HLC.tla     *)
(* MCHLC_legacy_sharded_gap.cfg).  On the unfenced apply paths (raw        *)
(* writes: handleRawRequest → ApplyMutationsRaftAt with startTS =          *)
(* commitTS) a version AT commit_ts is not a conflict, and the new value   *)
(* is written to the same Pebble key, replacing the acknowledged one.      *)
(* (Raft replays are told apart by applied index, staleRaftApplyFastPath-  *)
(* Locked, and are not modelled.)                                          *)
(*                                                                         *)
(* Safety properties:                                                      *)
(*                                                                         *)
(*   MVCC-1  At most one version per (key, commit_ts) pair.                *)
(*   MVCC-4  Compaction does not lose committed entries: every read at     *)
(*           read_ts >= the effective watermark sees what the uncompacted *)
(*           chain would show.                                             *)
(*   MVCC-5  Every acknowledged write is still in the uncompacted chain    *)
(*           (no silent overwrite).  Violated when timestamps collide.    *)
(*                                                                         *)
(* MVCC-2 (no version below the HLC physical ceiling) and MVCC-3           *)
(* (cross-node read consistency) are not modelled here: MVCC-2 belongs    *)
(* with HLC.tla and MVCC-3 needs a multi-node model.                       *)
(*                                                                         *)
(* EnableSafety toggles the "keep the newest version <= minTS" rule so the *)
(* gap config surfaces a MVCC-4 counterexample.                            *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets

CONSTANTS
    Keys,           \* finite set of keys exercised in the model
    Vals,           \* finite set of stored values
    MaxTs,          \* upper bound on the abstract HLC counter (state-space)
    MaxOps,         \* upper bound on Write actions per run (state-space)
    EnableSafety,   \* TRUE: encode MVCC-4 retention guard; FALSE: gap config
    UniqueTs        \* TRUE: write timestamps never collide; FALSE: G10 collisions

VARIABLES
    versions,         \* Keys -> SUBSET [commitTs: 0..MaxTs, value: Vals]
                      \* The actual store.  PruneKey prunes from it.
    originalVersions, \* Ghost: the version chain with no compaction.  Writes
                      \* apply to it exactly as to `versions`; PruneKey never
                      \* touches it.  MVCC-4 compares the two.
    acked,            \* Ghost: every acknowledged write [key, commitTs, value].
    tsCounter,        \* Nat. Monotonic clock; advances on Write.
    minRetained,      \* Nat. Committed compaction watermark.
    pendingMin,       \* Nat. Pending compaction watermark (0 = none).
    pruned,           \* SUBSET Keys. Keys pruned under pendingMin.
    opCount           \* state-space bound

vars == <<versions, originalVersions, acked, tsCounter, minRetained,
          pendingMin, pruned, opCount>>

\* === HELPERS ===

\* The visible version at read_ts in a version-set: the entry with the
\* largest commit_ts <= read_ts.  Returns the sentinel record [commitTs |->
\* 0, value |-> "_none_"] when no such version exists.  We pick a record
\* type so the same expression can be used on either operand in MVCC-4.
NotFound == [commitTs |-> 0, value |-> "_none_"]

LatestBefore(S, ts) ==
    LET below == { v \in S : v.commitTs <= ts /\ v.commitTs > 0 } IN
    IF below = {} THEN NotFound
    ELSE CHOOSE v \in below : \A w \in below : w.commitTs <= v.commitTs

\* effectiveMinRetainedTS: the pending watermark once one is persisted.
EffectiveMin == IF pendingMin > minRetained THEN pendingMin ELSE minRetained

\* Replace whatever version of k sits at ts with [ts, v].
PutVersion(S, ts, v) ==
    { w \in S : w.commitTs # ts } \cup {[commitTs |-> ts, value |-> v]}

\* === INIT ===
Init ==
    /\ versions         = [k \in Keys |-> {}]
    /\ originalVersions = [k \in Keys |-> {}]
    /\ acked            = {}
    /\ tsCounter        = 0
    /\ minRetained      = 0
    /\ pendingMin       = 0
    /\ pruned           = {}
    /\ opCount          = 0

\* === ACTIONS ===

(***************************************************************************)
(* Write(k, v): allocate the next ts from the monotonic clock and apply a  *)
(* version to both the actual `versions[k]` and the ghost chain.          *)
(***************************************************************************)
Write(k, v) ==
    /\ opCount < MaxOps
    /\ tsCounter < MaxTs
    /\ LET newTs == tsCounter + 1 IN
        /\ tsCounter'        = newTs
        /\ versions'         = [versions         EXCEPT ![k] = PutVersion(@, newTs, v)]
        /\ originalVersions' = [originalVersions EXCEPT ![k] = PutVersion(@, newTs, v)]
        /\ acked'            = acked \cup {[key |-> k, commitTs |-> newTs, value |-> v]}
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<minRetained, pendingMin, pruned>>

(***************************************************************************)
(* CollidingWrite(k, v, ts): a write stamped with a timestamp that already *)
(* has a version of k (UniqueTs = FALSE only).  The raw apply path finds   *)
(* no version above ts, so it is not a conflict, and the write replaces   *)
(* the existing version; the client is acknowledged.                       *)
(***************************************************************************)
CollidingWrite(k, v, ts) ==
    /\ ~UniqueTs
    /\ opCount < MaxOps
    /\ \E w \in versions[k] : w.commitTs = ts
    /\ ~\E w \in versions[k] : w.commitTs > ts    \* checkApplyConflicts(startTS = ts)
    /\ versions'         = [versions         EXCEPT ![k] = PutVersion(@, ts, v)]
    /\ originalVersions' = [originalVersions EXCEPT ![k] = PutVersion(@, ts, v)]
    /\ acked'            = acked \cup {[key |-> k, commitTs |-> ts, value |-> v]}
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<tsCounter, minRetained, pendingMin, pruned>>

(***************************************************************************)
(* BeginCompact(newMin): persist the pending watermark.  From here reads   *)
(* below newMin fail closed.                                               *)
(***************************************************************************)
BeginCompact(newMin) ==
    /\ pendingMin = 0
    /\ newMin > minRetained
    /\ newMin <= tsCounter
    /\ pendingMin' = newMin
    /\ pruned'     = {}
    /\ UNCHANGED <<versions, originalVersions, acked, tsCounter, minRetained, opCount>>

(***************************************************************************)
(* PruneKey(k): delete k's garbage under the pending watermark.            *)
(*                                                                         *)
(* Under EnableSafety the prune keeps every version with commit_ts >      *)
(* pendingMin plus the newest version with commit_ts <= pendingMin, so a   *)
(* read at any read_ts >= pendingMin still sees what it saw before.        *)
(* Under ~EnableSafety (gap config) the newest version <= pendingMin is    *)
(* dropped too, and TLC surfaces a MVCC-4 counterexample.                  *)
(***************************************************************************)
RetainOnCompact(k, minTS) ==
    LET above  == { v \in versions[k] : v.commitTs > minTS }
        newest == LatestBefore(versions[k], minTS)
    IN IF ~EnableSafety \/ newest = NotFound
       THEN above
       ELSE above \cup {newest}

PruneKey(k) ==
    /\ pendingMin > 0
    /\ versions' = [versions EXCEPT ![k] = RetainOnCompact(k, pendingMin)]
    /\ pruned'   = pruned \cup {k}
    /\ UNCHANGED <<originalVersions, acked, tsCounter, minRetained, pendingMin, opCount>>

(***************************************************************************)
(* FinishCompact: the GC scan reached EOF; commit the watermark.           *)
(***************************************************************************)
FinishCompact ==
    /\ pendingMin > 0
    /\ pruned = Keys
    /\ minRetained' = pendingMin
    /\ pendingMin'  = 0
    /\ pruned'      = {}
    /\ UNCHANGED <<versions, originalVersions, acked, tsCounter, opCount>>

\* === NEXT ===
Next ==
    \/ \E k \in Keys, v \in Vals : Write(k, v)
    \/ \E k \in Keys, v \in Vals, ts \in 1..MaxTs : CollidingWrite(k, v, ts)
    \/ \E newMin \in 1..MaxTs : BeginCompact(newMin)
    \/ \E k \in Keys : PruneKey(k)
    \/ FinishCompact

Spec == Init /\ [][Next]_vars

\* === STATE CONSTRAINT ===
StateConstraint ==
    /\ tsCounter   <= MaxTs
    /\ minRetained <= MaxTs
    /\ opCount     <= MaxOps

\* === TYPE INVARIANT ===
TypeOK ==
    /\ tsCounter        \in 0..MaxTs
    /\ minRetained      \in 0..MaxTs
    /\ pendingMin       \in 0..MaxTs
    /\ pruned           \subseteq Keys
    /\ opCount          \in 0..MaxOps
    /\ versions         \in [Keys -> SUBSET [commitTs: 0..MaxTs, value: Vals]]
    /\ originalVersions \in [Keys -> SUBSET [commitTs: 0..MaxTs, value: Vals]]
    /\ acked            \subseteq [key: Keys, commitTs: 0..MaxTs, value: Vals]

\* === SAFETY INVARIANTS ===

\* MVCC-1 — uniqueness: no two distinct version records in versions[k]
\* share a commit_ts.  The store keys versions by (key, commit_ts), so a
\* second write at the same pair replaces the first rather than adding a
\* record; this holds in every config.
MVCC1_VisibleVersionUnique ==
    \A k \in Keys :
        \A v1, v2 \in versions[k] :
            v1.commitTs = v2.commitTs => v1 = v2

\* MVCC-4 — no lost commit on compaction.  For every key k and every
\* read_ts at or above the effective watermark, the version visible in
\* `versions[k]` matches the version visible in the uncompacted chain.
\* read_ts below the effective watermark is out of scope — those reads
\* fail with ErrReadTSCompacted.
MVCC4_NoLostCommitOnSnapshotInstall ==
    \A k \in Keys, ts \in EffectiveMin..MaxTs :
        LatestBefore(versions[k], ts) = LatestBefore(originalVersions[k], ts)

\* MVCC-5 — every acknowledged write is still in the uncompacted chain.
\* Compaction may drop it later (MVCC-4 covers that); a later write at
\* the same timestamp must not.  Fails with UniqueTs = FALSE.
MVCC5_AckedWriteNotOverwritten ==
    \A a \in acked :
        [commitTs |-> a.commitTs, value |-> a.value] \in originalVersions[a.key]

\* === ACTION-LEVEL PROPERTIES ===

\* tsCounter never decreases.
MVCC_TsMonotonic ==
    [][tsCounter' >= tsCounter]_vars

\* The uncompacted chain never loses a timestamp — writes only add or
\* replace, and compaction leaves it UNCHANGED.  This is the property
\* that makes MVCC4_NoLostCommitOnSnapshotInstall a meaningful witness
\* rather than a vacuous claim about an empty ghost.
MVCC_GhostMonotonic ==
    [][\A k \in Keys :
         {v.commitTs : v \in originalVersions[k]} \subseteq
         {v.commitTs : v \in originalVersions'[k]}]_vars

\* The effective watermark never decreases (reads that once failed closed
\* never become answerable again).
MVCC_WatermarkMonotonic ==
    [][EffectiveMin' >= EffectiveMin]_vars

=============================================================================
