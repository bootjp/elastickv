-------------------------------- MODULE MCHLC --------------------------------
(***************************************************************************)
(* Model-check instance for HLC.tla.                                       *)
(*                                                                         *)
(* Two .cfg files point at this module:                                    *)
(*   MCHLC.cfg      — EnableSafety = TRUE  (correct design; TLC passes)    *)
(*   MCHLC_gap.cfg  — EnableSafety = FALSE (no preconditions; TLC fails    *)
(*                    on HLC-4 with a counterexample, demonstrating the    *)
(*                    design gap the spec proposal exists to surface)      *)
(*                                                                         *)
(* Run from the repository root:                                           *)
(*   make tla-check                                                        *)
(***************************************************************************)

EXTENDS HLC, TLC

\* TLC symmetry hint: nodes are interchangeable in HLC.tla (per the spec
\* §6.3 per-module symmetry — HLC alone is node-symmetric).
NodeSymmetry == Permutations(Nodes)

=============================================================================
