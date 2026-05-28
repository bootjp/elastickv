-------------------------------- MODULE MCOCC --------------------------------
(***************************************************************************)
(* Model-check instance for OCC.tla.                                       *)
(*                                                                         *)
(* Two .cfg files point at this module:                                    *)
(*   MCOCC.cfg      — EnableSafety = TRUE  (correct design; TLC passes)    *)
(*   MCOCC_gap.cfg  — EnableSafety = FALSE (Commit's OCC-1 guard removed;  *)
(*                    TLC fails on OCC1_CommitTsAboveStart and emits a     *)
(*                    counterexample, motivating the spec's existence)     *)
(*                                                                         *)
(* Run via `make tla-check` from the repository root.                      *)
(***************************************************************************)

EXTENDS OCC, TLC

\* TLC symmetry hint: TxnIds and Keys are interchangeable in OCC.tla
\* per the spec doc §6.3 (OCC alone is both txn- and key-symmetric —
\* keys participate only as indices, not in an order).
TxnSymmetry == Permutations(TxnIds)
KeySymmetry == Permutations(Keys)
Symmetry    == TxnSymmetry \cup KeySymmetry

=============================================================================
