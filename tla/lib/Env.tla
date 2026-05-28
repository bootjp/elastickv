-------------------------------- MODULE Env ----------------------------------
(***************************************************************************)
(* Shared environment constants used by every module in the elastickv      *)
(* TLA+ suite. Per docs/design/2026_05_28_proposed_tla_safety_spec.md      *)
(* §6.1.                                                                    *)
(*                                                                         *)
(* The single normative ASSUME below encodes HLC-4 precondition (i) — the  *)
(* bounded-skew assumption.  Modules that follow (Raft, HLC, …) EXTEND     *)
(* this module so the assumption is in scope wherever it is needed.        *)
(***************************************************************************)

EXTENDS Naturals

CONSTANTS
    Nodes,                  \* finite set of node identities
    MaxClockSkewMs,         \* upper bound on inter-node wall clock skew (ms)
    HlcPhysicalWindowMs,    \* hlcPhysicalWindowMs in kv/sharded_coordinator.go
    MaxTerms,               \* state-space bound: number of leadership terms
    MaxOps,                 \* state-space bound: number of persistence Next() calls
    MaxWallTime,            \* state-space bound: largest wall clock value
    LogicalMax              \* logical-counter wrap bound (real impl: 65535)

\* HLC-4 precondition (i): the renewed ceiling outlives any inter-node skew.
\* See HLC.tla and the design doc §5.1 (i) for the hazard this rules out.
ASSUME MaxClockSkewMs < HlcPhysicalWindowMs

\* Useful sets
Terms == 1..MaxTerms

\* Liveness sets (placeholders for future modules; trivial in M1)
LiveNodes == Nodes
=============================================================================
