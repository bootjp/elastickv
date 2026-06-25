-------------------------------- MODULE MCMVCC -------------------------------
(***************************************************************************)
(* Model-check instance for MVCC.tla.                                      *)
(*                                                                         *)
(* Two .cfg files point at this module:                                    *)
(*   MCMVCC.cfg      — EnableSafety = TRUE  (correct design; TLC passes)   *)
(*   MCMVCC_gap.cfg  — EnableSafety = FALSE (Compact drops the latest-     *)
(*                     before-newMin retention; TLC fails on               *)
(*                     MVCC4_NoLostCommitOnSnapshotInstall and emits a     *)
(*                     counterexample, motivating the retention guard)     *)
(*                                                                         *)
(* Run via `make tla-check` from the repository root.                      *)
(***************************************************************************)

EXTENDS MVCC, TLC

\* Keys are symmetric in MVCC.tla per §6.3 of the design doc — they
\* participate only as indices, not in any ordering on the invariants.
\* Vals are symmetric for the same reason.
\*
\* Symmetry must be a group of permutations closed under composition.
\* The union of two disjoint per-domain permutation groups is NOT
\* closed under composition (gemini HIGH on PR #865 surfaced this in
\* MCComposed; the same construction was duplicated here and in
\* MCOCC).  The correct group is the direct product on the disjoint
\* union Keys \cup Vals, expressed via the TLC `@@` operator
\* (function union over disjoint domains).
KeySymmetry == Permutations(Keys)
ValSymmetry == Permutations(Vals)
Symmetry    == { kSym @@ vSym :
                    kSym \in KeySymmetry,
                    vSym \in ValSymmetry }

=============================================================================
