-------------------------------- MODULE Raft ---------------------------------
(***************************************************************************)
(* Abstract Raft interface for the elastickv TLA+ suite.                   *)
(* Per docs/design/2026_05_28_partial_tla_safety_spec.md §3 / §6.1, Raft   *)
(* is modelled as a black box that delivers the standard log-matching +    *)
(* leader-completeness guarantees.  This module exposes the **interface**  *)
(* (leader / term / BecomeLeader / Apply / InstallSnapshot) — the internal *)
(* election / replication state machine is NOT modelled.  Modules that     *)
(* need Raft EXTEND or INSTANCE this module.                                *)
(*                                                                         *)
(* For M1 (HLC alone) only the leader / term part is exercised.  Apply /   *)
(* InstallSnapshot are stubs that later milestones (M2..M5) fill in.       *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, Env

VARIABLES
    currentTerm,    \* currentTerm[n] \in 0..MaxTerms : node n's view of the active term
    leaderOf,       \* leaderOf[t]    \in Nodes \cup {NoLeader} : leader elected for term t
    activeTerm      \* highest term that has had a leader elected (0 = pre-election)

raftVars == <<currentTerm, leaderOf, activeTerm>>

NoLeader == "no_leader"

\* All term indices reachable in the bounded model.
TermsExplored == 0..MaxTerms

\* === INIT ===
RaftInit ==
    /\ currentTerm = [n \in Nodes |-> 0]
    /\ leaderOf    = [t \in TermsExplored |-> NoLeader]
    /\ activeTerm  = 0

\* === ACTIONS ===

(***************************************************************************)
(* BecomeLeader(n): node n is elected leader of a fresh term.              *)
(* Models a successful Raft election as an atomic step — the underlying    *)
(* vote-counting protocol is abstracted away (NG4 in the spec doc).        *)
(***************************************************************************)
BecomeLeader(n) ==
    /\ activeTerm < MaxTerms
    /\ LET t == activeTerm + 1 IN
        /\ activeTerm'  = t
        /\ leaderOf'    = [leaderOf EXCEPT ![t] = n]
        /\ currentTerm' = [currentTerm EXCEPT ![n] = t]

(***************************************************************************)
(* IsLeader(n): predicate.  TRUE iff n is the leader of the currently      *)
(* active term.  Used as a guard wherever the spec restricts an action     *)
(* to the leader (e.g. HLC.IssueTimestamp).                                *)
(***************************************************************************)
IsLeader(n) ==
    /\ activeTerm > 0
    /\ leaderOf[activeTerm] = n

\* Convenience: the node leading the current term, or NoLeader.
CurrentLeader ==
    IF activeTerm = 0 THEN NoLeader ELSE leaderOf[activeTerm]

\* === STUBS FOR LATER MILESTONES ===
(***************************************************************************)
(* Propose / Apply / InstallSnapshot are part of the Raft interface that   *)
(* later milestones (M2 OCC, M3 MVCC, M5 Composed) plug into.  For M1 they *)
(* are left as comments rather than no-op actions to avoid expanding the   *)
(* HLC-only state space.                                                   *)
(***************************************************************************)

\* === ASSUMED PROPERTIES ===
(***************************************************************************)
(* Per NG4: the abstract Raft layer assumes log matching and leader        *)
(* completeness without proving them — those are properties of etcd/raft   *)
(* which we treat as a verified black box.  In M1 we exercise only the     *)
(* leader-uniqueness consequence, which is structural here: BecomeLeader   *)
(* sets exactly one entry in leaderOf per term.                            *)
(***************************************************************************)
LeaderUniquenessPerTerm ==
    \A t \in TermsExplored : t > 0 /\ leaderOf[t] # NoLeader =>
        leaderOf[t] \in Nodes

=============================================================================
