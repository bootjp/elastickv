------------------------------ MODULE MCComposed -----------------------------
(***************************************************************************)
(* Model-check instance for Composed.tla.                                  *)
(*                                                                         *)
(* Two .cfg files point at this module:                                    *)
(*   MCComposed.cfg     — EnableSafety = TRUE  (TLC passes)                *)
(*   MCComposed_gap.cfg — EnableSafety = FALSE (TLC fails on Composed-1   *)
(*                       and emits the canonical 4-step counterexample)   *)
(*                                                                         *)
(* Run via `make tla-check` from the repository root.                      *)
(***************************************************************************)

EXTENDS Composed, TLC

\* Symmetry hint.  Per §6.3 of the design doc, keys / groups / txns are
\* genuinely symmetric in Composed.tla — none participates in an
\* ordering on the invariants (Composed-1 references routes[v][k] = g
\* but treats k and g as indices; Composed-3 only compares commit_ts
\* values, not txn identities).
KeySymmetry   == Permutations(Keys)
GroupSymmetry == Permutations(Groups)
TxnSymmetry   == Permutations(TxnIds)
Symmetry      == KeySymmetry \cup GroupSymmetry \cup TxnSymmetry

=============================================================================
