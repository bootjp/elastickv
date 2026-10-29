--------------------------------- MODULE HLC ---------------------------------
(***************************************************************************)
(* TLA+ specification of the elastickv Hybrid Logical Clock.               *)
(* Per docs/design/2026_05_28_implemented_tla_safety_spec.md §5.1.             *)
(*                                                                         *)
(* This module encodes the HLC layer that sits on top of an abstract Raft *)
(* (lib/Raft.tla) and follows the code on main:                            *)
(*                                                                         *)
(*   - IssueTimestamp is kv/hlc.go NextFenced: the physical half floors   *)
(*     to the Raft-applied ceiling; the call fails closed when the wall   *)
(*     clock has reached the ceiling or when the next value's physical    *)
(*     half would exceed it; no fence applies before the first ceiling    *)
(*     (ceiling = 0).                                                     *)
(*   - Nothing observes the previous term's timestamps on election.  The  *)
(*     strategy (c) handoff (Observe(MaxAppliedHLC) in BecomeLeader) was   *)
(*     never implemented, and the model no longer pretends it was.        *)
(*   - Who issues, and what carries the logical half across a leader     *)
(*     change, depends on the timestamp mode (CONSTANT IssuanceMode):     *)
(*                                                                         *)
(*       "tso"            Centralized TSO (kv/tso_raft.go,               *)
(*                        RaftTSOAllocator).  Only the verified leader of *)
(*                        the timestamp group reserves, it Observes the   *)
(*                        Raft-committed allocation floor before          *)
(*                        reserving (reserveLocalWindow), and it commits  *)
(*                        the new floor (commitAllocationFloor) before    *)
(*                        handing the window out.  This is strategy (a)  *)
(*                        of the design doc, and the mode the safe config *)
(*                        checks.                                          *)
(*       "legacy_single"  Non-sharded Coordinate (demo): leader-only      *)
(*                        issuance, no handoff.                            *)
(*       "legacy_sharded" ShardedCoordinator in legacy TSO mode (the      *)
(*                        default): the dispatching node stamps from its  *)
(*                        own HLC whether or not it leads (audit gap G10  *)
(*                        in docs/design/2026_09_26_proposed_serializable_ *)
(*                        isolation_audit.md), no handoff.                 *)
(*                                                                         *)
(* EnableSafety gates the TSO allocation-floor handoff, so the gap config *)
(* shows the floor is load-bearing for HLC-4.  The two legacy modes are   *)
(* checked by their own configs, which are expected to fail: they pin the *)
(* known implementation gaps so a fix shows up as an "unexpectedly        *)
(* passed" harness error that forces the model to be updated.            *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, Env, Raft

CONSTANTS
    EnableSafety,   \* TRUE: TSO allocation-floor handoff on; FALSE: gap config
    IssuanceMode    \* "tso" | "legacy_single" | "legacy_sharded"

IssuanceModes == {"tso", "legacy_single", "legacy_sharded"}

ASSUME IssuanceMode \in IssuanceModes

VARIABLES
    hlcLast,         \* hlcLast[n] = [wall |-> Nat, logical |-> 0..LogicalMax]
    physicalCeiling, \* physicalCeiling[n] : Nat  (Raft-applied)
    tsoFloor,        \* Raft-committed TSO allocation floor (kv/tso_fsm.go allocationFloor)
    wallNow,         \* wallNow[n] : Nat  (per-node wall clock)
    committedTS,     \* set of [ts, term, node, seq] records — every persistence Next() result
    opCount          \* state-space bound: number of IssueTimestamp calls so far

hlcVars == <<hlcLast, physicalCeiling, tsoFloor, wallNow, committedTS, opCount>>
vars    == <<raftVars, hlcVars>>

\* === DATA TYPES ===
HLCVal(w, l) == [wall |-> w, logical |-> l]

HLCLE(x, y) ==
    \/ x.wall < y.wall
    \/ (x.wall = y.wall /\ x.logical <= y.logical)

HLCLT(x, y) ==
    \/ x.wall < y.wall
    \/ (x.wall = y.wall /\ x.logical < y.logical)

HLCMax(x, y) == IF HLCLT(x, y) THEN y ELSE x

\* === HLC.NextFenced() COMPUTATION (mirrors kv/hlc.go) ===

\* fencedNowMillis: before the first ceiling (0) the wall clock is used
\* unfenced; afterwards the wall clock is floored to the ceiling.  The
\* "wall clock reached the ceiling" rejection is FenceOpen below.
FlooredNow(n) ==
    IF wallNow[n] < physicalCeiling[n] THEN physicalCeiling[n] ELSE wallNow[n]

\* nextLocked: the (nowMs > prevWall) and (nowMs <= prevWall) branches,
\* including the logical-overflow physical bump.
ComputeNextHLC(prev, flooredNow) ==
    IF flooredNow > prev.wall THEN
        HLCVal(flooredNow, 0)
    ELSE IF prev.logical < LogicalMax THEN
        HLCVal(prev.wall, prev.logical + 1)
    ELSE
        HLCVal(prev.wall + 1, 0)  \* logical overflow bumps wall by 1

\* The NextFenced gate: no fence while ceiling = 0; otherwise the wall
\* clock must be below the ceiling (fencedNowMillis) and the candidate's
\* physical half must not exceed it (rejectFencedPhysicalOverflow).
FenceOpen(n, ts) ==
    \/ physicalCeiling[n] = 0
    \/ /\ wallNow[n] < physicalCeiling[n]
       /\ ts.wall <= physicalCeiling[n]

\* Which node may stamp a persistence timestamp in this mode.
MayIssue(n) ==
    IF IssuanceMode = "legacy_sharded"
    THEN activeTerm > 0          \* any node that can reach a leader stamps locally
    ELSE IsLeader(n)

\* The value NextFenced starts from.  In TSO mode the reservation first
\* Observes the committed allocation floor (reserveLocalWindow →
\* clock.Observe(minimum)).
IssueBase(n) ==
    IF IssuanceMode = "tso" /\ EnableSafety
    THEN HLCMax(hlcLast[n], tsoFloor)
    ELSE hlcLast[n]

\* === INIT ===
HLCInit ==
    /\ hlcLast         = [n \in Nodes |-> HLCVal(0, 0)]
    /\ physicalCeiling = [n \in Nodes |-> 0]
    /\ tsoFloor        = HLCVal(0, 0)
    /\ wallNow         = [n \in Nodes |-> 0]
    /\ committedTS     = {}
    /\ opCount         = 0

Init == RaftInit /\ HLCInit

\* === ACTIONS ===

(***************************************************************************)
(* BecomeLeader_HLC(n) — Raft elects n as the new term's leader.  No HLC  *)
(* state changes: the implementation has no election-time Observe.  In    *)
(* TSO mode the handoff happens at the next reservation (IssueBase).      *)
(***************************************************************************)
BecomeLeader_HLC(n) ==
    /\ BecomeLeader(n)
    /\ UNCHANGED hlcVars

(***************************************************************************)
(* TickWall(n) — n's wall clock advances by 1 ms.  Bounded skew is enforced *)
(* against every other node (HLC-4 precondition (i) governs how big the   *)
(* allowed gap is).  MaxWallTime caps state-space exploration.             *)
(***************************************************************************)
TickWall(n) ==
    /\ wallNow[n] < MaxWallTime
    \* Bounded inter-node skew (HLC-4 precondition (i)).  Stated with addition
    \* rather than subtraction so the inequality stays inside Nat (per gemini
    \* PR #856 review: subtraction in Naturals is undefined when the LHS is
    \* smaller and would halt TLC).
    \* Self is excluded from the skew check: the bounded-skew assumption
    \* is about *inter*-node disagreement, not the ticking node's own
    \* clock relative to its post-tick value.  Without this exclusion
    \* the constraint includes `wallNow[n] + 1 <= wallNow[n] + MaxClockSkewMs`
    \* which is unsatisfiable whenever MaxClockSkewMs = 0 — disabling
    \* TickWall entirely and preventing the model from ever exercising
    \* wall-clock advancement (codex P2 on PR #856 round 2).
    /\ \A m \in Nodes \ {n} :
        /\ wallNow[n] + 1 <= wallNow[m] + MaxClockSkewMs
        /\ wallNow[m] <= wallNow[n] + 1 + MaxClockSkewMs
    /\ wallNow' = [wallNow EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<raftVars, hlcLast, physicalCeiling, tsoFloor, committedTS, opCount>>

(***************************************************************************)
(* ApplyCeiling(n) — the leader proposes a fresh ceiling and Raft applies *)
(* it on every node (RunHLCLeaseRenewal / RecoverHLCLease →               *)
(* SetPhysicalCeiling).  Modelled atomically: every node's ceiling is     *)
(* raised to max(current, wallNow[leader] + HlcPhysicalWindowMs).  HLC-2  *)
(* (ceiling monotonicity) is preserved by the max().                      *)
(***************************************************************************)
ApplyCeiling(n) ==
    /\ IsLeader(n)
    /\ LET newC == wallNow[n] + HlcPhysicalWindowMs IN
        /\ physicalCeiling' = [m \in Nodes |-> IF physicalCeiling[m] < newC
                                                THEN newC
                                                ELSE physicalCeiling[m]]
    /\ UNCHANGED <<raftVars, hlcLast, tsoFloor, wallNow, committedTS, opCount>>

(***************************************************************************)
(* IssueTimestamp(n) — issue a persistence HLC ts through NextFenced.     *)
(*   - MayIssue(n): leader-only, except in "legacy_sharded" (G10).        *)
(*   - FenceOpen: the HLC-4 (iii) ceiling fence, always on in the code.    *)
(*   - In TSO mode the reservation starts from the committed floor and    *)
(*     commits the new floor before the timestamp is returned; the two    *)
(*     Raft round trips are collapsed into this one step.                  *)
(* committedTS records (ts, term) for the HLC-3 / HLC-4 checks.           *)
(***************************************************************************)
IssueTimestamp(n) ==
    /\ MayIssue(n)
    /\ opCount < MaxOps                     \* state-space bound
    /\ LET prev == IssueBase(n)
           ts   == ComputeNextHLC(prev, FlooredNow(n))
        IN /\ FenceOpen(n, ts)
           /\ hlcLast' = [hlcLast EXCEPT ![n] = ts]
           /\ tsoFloor' = IF IssuanceMode = "tso" /\ EnableSafety
                          THEN HLCMax(tsoFloor, ts)
                          ELSE tsoFloor
           \* Each commit gets the global opCount as a strict issuance
           \* ordinal so HLC1_PerNodeMonotonic can talk about "the prev
           \* and next commit by the same node" — without it the state
           \* invariant would just be a tautology on a totally ordered set.
           /\ committedTS' = committedTS \cup
                {[ts |-> ts, term |-> activeTerm, node |-> n, seq |-> opCount]}
    /\ opCount' = opCount + 1
    /\ UNCHANGED <<raftVars, physicalCeiling, wallNow>>

\* === NEXT ===
Next ==
    \/ \E n \in Nodes : BecomeLeader_HLC(n)
    \/ \E n \in Nodes : ApplyCeiling(n)
    \/ \E n \in Nodes : IssueTimestamp(n)
    \/ \E n \in Nodes : TickWall(n)

Spec == Init /\ [][Next]_vars

\* === STATE CONSTRAINT (TLC pruning) ===
StateConstraint ==
    /\ opCount <= MaxOps
    /\ activeTerm <= MaxTerms
    /\ \A n \in Nodes : wallNow[n] <= MaxWallTime
    /\ \A n \in Nodes : hlcLast[n].wall <= MaxWallTime + HlcPhysicalWindowMs + 1
    /\ \A n \in Nodes : physicalCeiling[n] <= MaxWallTime + HlcPhysicalWindowMs

\* === TYPE INVARIANT ===
HLCType == [wall : Nat, logical : 0..LogicalMax]

TypeOK ==
    /\ hlcLast \in [Nodes -> HLCType]
    /\ physicalCeiling \in [Nodes -> Nat]
    /\ tsoFloor \in HLCType
    /\ wallNow \in [Nodes -> Nat]
    /\ opCount \in 0..MaxOps
    /\ \A r \in committedTS :
        /\ r.ts \in HLCType
        /\ r.term \in 1..MaxTerms
        /\ r.node \in Nodes
        /\ r.seq \in 0..MaxOps

\* === SAFETY INVARIANTS ===

\* HLC-1 — per-node monotonicity of hlcLast across IssueTimestamp /
\* Observe.  The earlier formulation `HLCLE(r1, r2) \/ HLCLE(r2, r1)` was
\* vacuously true under the total lexicographic order on HLCVal (codex
\* PR #856 P2): it would have admitted a regression (5,0) -> (4,0) as
\* "ordered".  We instead carry an explicit issuance ordinal on every
\* committedTS record (`seq` is the global opCount at the moment
\* IssueTimestamp fired) and assert strict-greater on the same node —
\* every later issuance is strictly above the previous one.  Combined
\* with the per-action invariant HLC1_Action below this gives the same
\* monotonicity the implementation provides via h.last's CAS.
HLC1_PerNodeMonotonic ==
    \A r1, r2 \in committedTS :
        (r1.node = r2.node /\ r1.seq < r2.seq) => HLCLT(r1.ts, r2.ts)

\* HLC-2 — physical ceiling monotonicity is fundamentally an *action*
\* invariant: physicalCeiling[n] must not decrease across any step.  We
\* state it as the safety property `HLC2_Action` below and reference it
\* via PROPERTIES in MCHLC.cfg.  No state-level form is meaningful here
\* (the earlier `>= 0` was vacuously implied by TypeOK — codex PR #856
\* P2).
HLC2_NonNeg == \A n \in Nodes : physicalCeiling[n] \in Nat

\* === ACTION-LEVEL INVARIANTS ===
(***************************************************************************)
(* Action-level safety properties.  In TLA+ idiom these are written as     *)
(* [][P]_vars and registered under PROPERTIES (not INVARIANTS) in the .cfg *)
(* — TLC checks them as transition properties without requiring fairness.  *)
(***************************************************************************)

\* HLC-1 (transition form): hlcLast on every node weakly increases across
\* every step.  Built into ComputeNextHLC and the TSO floor Observe; this
\* property pins the contract.
HLC1_Action ==
    [][\A n \in Nodes : HLCLE(hlcLast[n], hlcLast'[n])]_vars

\* HLC-2 (transition form): physicalCeiling on every node weakly increases
\* across every step.  ApplyCeiling raises with max(); no action lowers it.
HLC2_Action ==
    [][\A n \in Nodes : physicalCeiling[n] <= physicalCeiling'[n]]_vars

\* HLC-3 — only the leader of a term issues commits for that term.  Holds
\* in "tso" and "legacy_single"; "legacy_sharded" violates it (G10).
HLC3_LeaderOnly ==
    \A r \in committedTS :
        r.term > 0 /\ leaderOf[r.term] = r.node

\* HLC-4 — every commit in a later term is strictly greater than every
\* commit in any earlier term.  The load-bearing invariant: the TSO
\* allocation floor enforces it; without the floor (gap config, or either
\* legacy mode) TLC surfaces a counterexample.
HLC4_NoRegressionAcrossTerms ==
    \A r1, r2 \in committedTS :
        r1.term < r2.term => HLCLT(r1.ts, r2.ts)

=============================================================================
