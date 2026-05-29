--------------------------------- MODULE MVCC --------------------------------
(***************************************************************************)
(* TLA+ specification of the elastickv MVCC layer.                         *)
(* Per docs/design/2026_05_28_partial_tla_safety_spec.md §5.3.             *)
(*                                                                         *)
(* Models a per-key version chain `versions[k]` and a `Compact` action     *)
(* that drains older versions to bound storage growth.  The model focuses  *)
(* on two safety properties:                                                *)
(*                                                                         *)
(*   MVCC-1  At most one version per (key, commit_ts) pair.                *)
(*   MVCC-4  Snapshot install / compaction does NOT lose committed entries *)
(*           — every commit_ts in (-inf, minRetained] is still visible at *)
(*           any read_ts >= minRetained that would have seen it before     *)
(*           the compaction.                                                *)
(*                                                                         *)
(* MVCC-2 (no version below the HLC physical ceiling) and MVCC-3           *)
(* (cross-node read consistency) are deferred to M5 — they belong with     *)
(* HLC.tla (MVCC-2) and the multi-node composed spec (MVCC-3) respectively. *)
(*                                                                         *)
(* The single CONSTANT `EnableSafety` toggles the visibility-preservation  *)
(* guard inside `Compact`.  Under EnableSafety the action retains the      *)
(* largest commit_ts <= minRetained per key so that any later read at      *)
(* read_ts >= minRetained sees the same value the original log would have  *)
(* shown.  Under the gap config that guard is removed and TLC surfaces a   *)
(* MVCC-4 counterexample (the design doc's motivating evidence pattern,   *)
(* same shape used for HLC and OCC).                                       *)
(*                                                                         *)
(* HLC is abstracted to a single monotonic counter `tsCounter`; M5         *)
(* (composed) will INSTANCE HLC.tla for the real 48/16 layout.            *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets

CONSTANTS
    Keys,           \* finite set of keys exercised in the model
    Vals,           \* finite set of stored values
    MaxTs,          \* upper bound on the abstract HLC counter (state-space)
    MaxOps,         \* upper bound on Write actions per run (state-space)
    EnableSafety    \* TRUE: encode MVCC-4 retention guard; FALSE: gap config

VARIABLES
    versions,         \* Keys -> SUBSET [commitTs: 0..MaxTs, value: Vals]
                      \* The actual store.  Compact prunes from it.
    originalVersions, \* Ghost: every version ever Written.  Compact NEVER prunes
                      \* this — MVCC-4 compares it against `versions` to assert
                      \* that no committed entry was lost.
    tsCounter,        \* Nat. Monotonic clock; advances on Write.
    minRetained,      \* Nat. Compaction watermark.  Reads at read_ts <
                      \* minRetained are no longer answerable; reads at
                      \* read_ts >= minRetained must still see the same value
                      \* they would have seen before Compact (MVCC-4).
    opCount           \* state-space bound

vars == <<versions, originalVersions, tsCounter, minRetained, opCount>>

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

\* === INIT ===
Init ==
    /\ versions         = [k \in Keys |-> {}]
    /\ originalVersions = [k \in Keys |-> {}]
    /\ tsCounter        = 0
    /\ minRetained      = 0
    /\ opCount          = 0

\* === ACTIONS ===

(***************************************************************************)
(* Write(k, v): allocate the next ts from the monotonic clock and append   *)
(* a version to both the actual `versions[k]` and the ghost              *)
(* `originalVersions[k]`.  The ghost diverges from the actual only       *)
(* after Compact runs.                                                     *)
(***************************************************************************)
Write(k, v) ==
    /\ opCount < MaxOps
    /\ tsCounter < MaxTs
    /\ LET newTs == tsCounter + 1
           ver   == [commitTs |-> newTs, value |-> v]
        IN /\ tsCounter' = newTs
           /\ versions'         = [versions         EXCEPT ![k] = @ \cup {ver}]
           /\ originalVersions' = [originalVersions EXCEPT ![k] = @ \cup {ver}]
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<minRetained>>

(***************************************************************************)
(* Compact(newMin): advance the retention watermark to newMin and prune    *)
(* `versions`.                                                              *)
(*                                                                         *)
(* Under EnableSafety, the per-key prune retains:                          *)
(*   - every version with commit_ts >= newMin (still in the active range), *)
(*   - PLUS the single largest version with commit_ts < newMin (so reads   *)
(*     at any read_ts in [newMin, MaxTs] still see the same value the     *)
(*     original log would have shown).                                     *)
(*                                                                         *)
(* Under ~EnableSafety (gap config), the prune drops EVERY version with    *)
(* commit_ts < newMin including the latest-before-newMin retention.  TLC  *)
(* surfaces a MVCC-4 counterexample: a key k has a Write at commit_ts < *)
(* newMin, no Write at commit_ts in [newMin, ...], and a read at         *)
(* read_ts >= newMin would now see NotFound instead of the original       *)
(* value.                                                                  *)
(***************************************************************************)
RetainOnCompact(k, newMin) ==
    LET aboveOrAt == { v \in versions[k] : v.commitTs >= newMin }
        latestBelow == LatestBefore(versions[k], newMin - 1)
    IN IF latestBelow = NotFound
       THEN aboveOrAt
       ELSE aboveOrAt \cup {latestBelow}

Compact(newMin) ==
    /\ newMin > minRetained
    /\ newMin <= tsCounter
    /\ minRetained' = newMin
    /\ versions' =
        [k \in Keys |->
            IF EnableSafety
            THEN RetainOnCompact(k, newMin)
            ELSE { v \in versions[k] : v.commitTs >= newMin }]
    /\ UNCHANGED <<originalVersions, tsCounter, opCount>>

\* === NEXT ===
Next ==
    \/ \E k \in Keys, v \in Vals : Write(k, v)
    \/ \E newMin \in 1..MaxTs    : Compact(newMin)

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
    /\ opCount          \in 0..MaxOps
    /\ versions         \in [Keys -> SUBSET [commitTs: 0..MaxTs, value: Vals]]
    /\ originalVersions \in [Keys -> SUBSET [commitTs: 0..MaxTs, value: Vals]]

\* === SAFETY INVARIANTS ===

\* MVCC-1 — uniqueness: no two distinct version records in versions[k]
\* share a commit_ts.  Equivalent to "Write never collides on
\* (key, commit_ts)".  Holds in both safe and gap configs because Write
\* allocates `newTs = tsCounter + 1` and tsCounter is strictly monotonic.
MVCC1_VisibleVersionUnique ==
    \A k \in Keys :
        \A v1, v2 \in versions[k] :
            v1.commitTs = v2.commitTs => v1 = v2

\* MVCC-4 — no lost commit on snapshot install / compaction.  For every
\* key k and every read_ts in [minRetained, MaxTs], the version visible
\* in `versions[k]` matches the version visible in `originalVersions[k]`
\* (the ghost set of every version ever Written).  read_ts < minRetained
\* is intentionally out of scope — Compact is allowed to make those reads
\* unanswerable, the contract is preservation at read_ts >= minRetained.
\*
\* Under EnableSafety the RetainOnCompact pre-image preserves the
\* latest-before-newMin entry per key, so the equality holds.  Under
\* gap the prune drops that entry and TLC produces a counterexample.
MVCC4_NoLostCommitOnSnapshotInstall ==
    \A k \in Keys, ts \in minRetained..MaxTs :
        LatestBefore(versions[k], ts) = LatestBefore(originalVersions[k], ts)

\* === ACTION-LEVEL PROPERTIES ===

\* tsCounter is strictly monotonic across every Write — and Compact
\* leaves it UNCHANGED.  Stated as a transition property; the load-bearing
\* MVCC-1 invariant is its state-level consequence.
MVCC_TsMonotonic ==
    [][tsCounter' >= tsCounter]_vars

\* originalVersions is monotonically non-shrinking — Write only adds and
\* Compact leaves it UNCHANGED.  This is the property that makes
\* MVCC4_NoLostCommitOnSnapshotInstall a meaningful witness rather than
\* a vacuous claim about an empty ghost.
MVCC_GhostMonotonic ==
    [][\A k \in Keys : originalVersions[k] \subseteq originalVersions'[k]]_vars

=============================================================================
