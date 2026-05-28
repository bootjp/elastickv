--------------------------------- MODULE HLC ---------------------------------
(***************************************************************************)
(* TLA+ specification of the elastickv Hybrid Logical Clock.               *)
(* Per docs/design/2026_05_28_proposed_tla_safety_spec.md §5.1.            *)
(*                                                                         *)
(* This module encodes the HLC layer that sits on top of an abstract Raft *)
(* (lib/Raft.tla).  All three HLC-4 preconditions are first-class:         *)
(*                                                                         *)
(*   (i)   bounded clock skew (Env.tla ASSUME)                             *)
(*   (ii)  strategy (c) logical-counter handoff — Observe(MaxAppliedHLC)   *)
(*         inside the BecomeLeader action                                  *)
(*   (iii) commit-time ceiling fencing — wallNow[n] < physicalCeiling[n]   *)
(*         as an enabling guard on IssueTimestamp                          *)
(*                                                                         *)
(* Both (ii) and (iii) are gated by the CONSTANT EnableSafety so the same  *)
(* spec drives the safe model-check (MCHLC.cfg, EnableSafety = TRUE) and   *)
(* the gap model-check (MCHLC_gap.cfg, EnableSafety = FALSE) that          *)
(* demonstrates the HLC-4 counterexample the design doc anticipates.       *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, Env, Raft

CONSTANT EnableSafety  \* TRUE: encode preconditions (ii) and (iii); FALSE: gap config

VARIABLES
    hlcLast,         \* hlcLast[n] = [wall |-> Nat, logical |-> 0..LogicalMax]
    physicalCeiling, \* physicalCeiling[n] : Nat  (Raft-applied)
    maxAppliedHLC,   \* [wall, logical] : maximum committed HLC across all terms (FSM-observable)
    wallNow,         \* wallNow[n] : Nat  (per-node wall clock)
    committedTS,     \* set of [ts, term] records — every persistence Next() result
    opCount          \* state-space bound: number of IssueTimestamp calls so far

hlcVars == <<hlcLast, physicalCeiling, maxAppliedHLC, wallNow, committedTS, opCount>>
vars    == <<raftVars, hlcVars>>

\* === DATA TYPES ===
HLCVal(w, l) == [wall |-> w, logical |-> l]

HLCLE(x, y) ==
    \/ x.wall < y.wall
    \/ (x.wall = y.wall /\ x.logical <= y.logical)

HLCLT(x, y) ==
    \/ x.wall < y.wall
    \/ (x.wall = y.wall /\ x.logical < y.logical)

\* === HLC.Next() COMPUTATION (mirrors kv/hlc.go) ===
\* nowMs = max(wallNow[n], physicalCeiling[n])
FlooredNow(n) ==
    IF wallNow[n] < physicalCeiling[n] THEN physicalCeiling[n] ELSE wallNow[n]

\* The HLC algorithm proper.  prev is the previously issued ts on this node.
\* See kv/hlc.go Next() — the two branches mirror the (nowMs > prevWall) and
\* (nowMs <= prevWall) cases, including the logical-overflow physical bump.
ComputeNextHLC(prev, flooredNow) ==
    IF flooredNow > prev.wall THEN
        HLCVal(flooredNow, 0)
    ELSE IF prev.logical < LogicalMax THEN
        HLCVal(prev.wall, prev.logical + 1)
    ELSE
        HLCVal(prev.wall + 1, 0)  \* logical overflow bumps wall by 1

\* === INIT ===
HLCInit ==
    /\ hlcLast         = [n \in Nodes |-> HLCVal(0, 0)]
    /\ physicalCeiling = [n \in Nodes |-> 0]
    /\ maxAppliedHLC   = HLCVal(0, 0)
    /\ wallNow         = [n \in Nodes |-> 0]
    /\ committedTS     = {}
    /\ opCount         = 0

Init == RaftInit /\ HLCInit

\* === ACTIONS ===

(***************************************************************************)
(* BecomeLeader_HLC(n) — Raft elects n as the new term's leader, then     *)
(* (under EnableSafety) applies strategy (c): hlcLast[n] is raised to     *)
(* max(hlcLast[n], maxAppliedHLC) by an in-memory Observe.  This is the   *)
(* M1 default per the design doc §5.1 HLC-4 (ii).                          *)
(***************************************************************************)
BecomeLeader_HLC(n) ==
    /\ BecomeLeader(n)
    /\ hlcLast' = IF EnableSafety /\ HLCLT(hlcLast[n], maxAppliedHLC)
                  THEN [hlcLast EXCEPT ![n] = maxAppliedHLC]
                  ELSE hlcLast
    /\ UNCHANGED <<physicalCeiling, maxAppliedHLC, wallNow, committedTS, opCount>>

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
    /\ \A m \in Nodes : /\ wallNow[n] + 1 <= wallNow[m] + MaxClockSkewMs
                        /\ wallNow[m] <= wallNow[n] + 1 + MaxClockSkewMs
    /\ wallNow' = [wallNow EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<raftVars, hlcLast, physicalCeiling, maxAppliedHLC, committedTS, opCount>>

(***************************************************************************)
(* ApplyCeiling(n) — the leader proposes a fresh ceiling and Raft applies *)
(* it on every node.  Modelled atomically: every node's physicalCeiling   *)
(* is raised to max(current, wallNow[leader] + HlcPhysicalWindowMs).      *)
(* HLC-2 (ceiling monotonicity) is preserved by the max().                *)
(***************************************************************************)
ApplyCeiling(n) ==
    /\ IsLeader(n)
    /\ LET newC == wallNow[n] + HlcPhysicalWindowMs IN
        /\ physicalCeiling' = [m \in Nodes |-> IF physicalCeiling[m] < newC
                                                THEN newC
                                                ELSE physicalCeiling[m]]
    /\ UNCHANGED <<raftVars, hlcLast, maxAppliedHLC, wallNow, committedTS, opCount>>

(***************************************************************************)
(* IssueTimestamp(n) — issue a persistence HLC ts.  Two normative gates:  *)
(*   HLC-3: only the active leader may call this.                          *)
(*   HLC-4 (iii) under EnableSafety: wallNow[n] < physicalCeiling[n]      *)
(*         (the ceiling fence; if the ceiling has expired the leader      *)
(*          fails closed and ApplyCeiling must run first).                 *)
(* The committedTS log records (ts, term) for later HLC-4 checking; the   *)
(* FSM-side maxAppliedHLC is bumped so a future BecomeLeader_HLC under    *)
(* EnableSafety inherits the highest committed ts.                         *)
(***************************************************************************)
IssueTimestamp(n) ==
    /\ IsLeader(n)
    /\ opCount < MaxOps                     \* state-space bound
    /\ \/ ~EnableSafety                     \* gap config: no fence
       \/ wallNow[n] < physicalCeiling[n]   \* precondition (iii) guard
    /\ LET prev == hlcLast[n]
           ts   == ComputeNextHLC(prev, FlooredNow(n))
        IN /\ hlcLast' = [hlcLast EXCEPT ![n] = ts]
           \* Each commit gets the global opCount as a strict issuance
           \* ordinal so HLC1_PerNodeMonotonic can talk about "the prev
           \* and next commit by the same node" — without it the state
           \* invariant would just be a tautology on a totally ordered set.
           /\ committedTS' = committedTS \cup
                {[ts |-> ts, term |-> activeTerm, node |-> n, seq |-> opCount]}
           /\ maxAppliedHLC' = IF HLCLT(maxAppliedHLC, ts)
                               THEN ts
                               ELSE maxAppliedHLC
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
    /\ \A n \in Nodes : hlcLast[n].wall <= MaxWallTime + 2
    /\ \A n \in Nodes : physicalCeiling[n] <= MaxWallTime + HlcPhysicalWindowMs

\* === TYPE INVARIANT ===
HLCType == [wall : Nat, logical : 0..LogicalMax]

TypeOK ==
    /\ hlcLast \in [Nodes -> HLCType]
    /\ physicalCeiling \in [Nodes -> Nat]
    /\ maxAppliedHLC \in HLCType
    /\ wallNow \in [Nodes -> Nat]
    /\ opCount \in 0..MaxOps
    /\ \A r \in committedTS :
        /\ r.ts \in HLCType
        /\ r.term \in 1..MaxTerms
        /\ r.node \in Nodes
        /\ r.seq \in 0..MaxOps

\* === SAFETY INVARIANTS ===

\* HLC-1 — per-node monotonicity of hlcLast across IssueTimestamp /
\* BecomeLeader_HLC / Observe.  The earlier formulation
\* `HLCLE(r1, r2) \/ HLCLE(r2, r1)` was vacuously true under the total
\* lexicographic order on HLCVal (codex PR #856 P2): it would have admitted
\* a regression (5,0) -> (4,0) as "ordered".  We instead carry an explicit
\* issuance ordinal on every committedTS record (`seq` is the global opCount
\* at the moment IssueTimestamp fired) and assert strict-greater on the same
\* node — every later issuance is strictly above the previous one.  Combined
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
\* every step.  Built into ComputeNextHLC and into BecomeLeader_HLC's
\* Observe; this property pins the contract.
HLC1_Action ==
    [][\A n \in Nodes : HLCLE(hlcLast[n], hlcLast'[n])]_vars

\* HLC-2 (transition form): physicalCeiling on every node weakly increases
\* across every step.  ApplyCeiling raises with max(); no action lowers it.
HLC2_Action ==
    [][\A n \in Nodes : physicalCeiling[n] <= physicalCeiling'[n]]_vars

\* HLC-3 — only the leader of a term issues commits for that term.  Built
\* into IssueTimestamp via the IsLeader(n) guard; asserted explicitly so
\* TLC reports a violation if anyone bypasses the guard.
HLC3_LeaderOnly ==
    \A r \in committedTS :
        r.term > 0 /\ leaderOf[r.term] = r.node

\* HLC-4 — every commit in a later term is strictly greater than every
\* commit in any earlier term.  This is the load-bearing invariant: when
\* EnableSafety = TRUE strategy (c) + the ceiling fence enforce it; when
\* EnableSafety = FALSE TLC is expected to surface a counterexample.
HLC4_NoRegressionAcrossTerms ==
    \A r1, r2 \in committedTS :
        r1.term < r2.term => HLCLT(r1.ts, r2.ts)

=============================================================================
