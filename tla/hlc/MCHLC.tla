-------------------------------- MODULE MCHLC --------------------------------
(***************************************************************************)
(* Model-check instance for HLC.tla.                                       *)
(*                                                                         *)
(* Four .cfg files point at this module:                                   *)
(*   MCHLC.cfg                    — centralized TSO mode with the          *)
(*                                  allocation-floor handoff (TLC passes)  *)
(*   MCHLC_gap.cfg                — TSO mode, floor handoff disabled (TLC  *)
(*                                  fails on HLC-4)                        *)
(*   MCHLC_legacy_single_gap.cfg  — known gap: non-sharded Coordinate has  *)
(*                                  no handoff (TLC fails on HLC-4)        *)
(*   MCHLC_legacy_sharded_gap.cfg — known gap G10: ShardedCoordinator      *)
(*                                  stamps on followers (TLC fails on      *)
(*                                  HLC-3)                                 *)
(*                                                                         *)
(* Run from the repository root:                                           *)
(*   make tla-check                                                        *)
(***************************************************************************)

EXTENDS HLC, TLC

\* TLC symmetry hint: nodes are interchangeable in HLC.tla (per the spec
\* §6.3 per-module symmetry — HLC alone is node-symmetric).
NodeSymmetry == Permutations(Nodes)

=============================================================================
