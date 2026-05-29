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
\*
\* Symmetry must be a group of permutations closed under composition.
\* The union of two disjoint per-domain permutation groups is NOT
\* closed under composition (gemini HIGH on PR #865 surfaced this in
\* MCComposed; the same construction was duplicated here and in
\* MCMVCC).  The correct group is the direct product on the disjoint
\* union TxnIds \cup Keys, expressed via the TLC `@@` operator
\* (function union over disjoint domains).
TxnSymmetry == Permutations(TxnIds)
KeySymmetry == Permutations(Keys)
Symmetry    == { tSym @@ kSym :
                    tSym \in TxnSymmetry,
                    kSym \in KeySymmetry }

=============================================================================
