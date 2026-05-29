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
KeySymmetry == Permutations(Keys)
ValSymmetry == Permutations(Vals)
Symmetry    == KeySymmetry \cup ValSymmetry

=============================================================================
