-------------------------------- MODULE MCOCC --------------------------------
(***************************************************************************)
(* Model-check instance for OCC.tla.                                       *)
(*                                                                         *)
(* .cfg files that point at this module:                                  *)
(*   MCOCC.cfg          — the model of main (TLC passes OCC-1/2/4/5)       *)
(*   MCOCC_onephase.cfg — single-shard path only (TLC passes everything,   *)
(*                        including OCC-3 and OCC-8)                       *)
(*   MCOCC_gap.cfg      — EnableSafety = FALSE (TLC fails on               *)
(*                        OCC1_CommitTsAboveStart)                         *)
(*   MCOCC_g0_gap.cfg   — known gap G0 (TLC fails on                       *)
(*                        OCC3_ReadSnapshotStability)                      *)
(*   MCOCC_g1_gap.cfg   — known gap G1 (TLC fails on OCC8_NoWriteSkew)     *)
(*   MCOCC_emptystore_gap.cfg — known gap: empty-store start is MaxUint64  *)
(*                        (TLC fails on OCC3b_EmptyStoreReadStability)     *)
(*   MCOCC_multigroup_gap.cfg — known gap: G0 across groups on the         *)
(*                        one-phase path (TLC fails on OCC-3)              *)
(*   MCOCC_live.cfg     — OCC-L1 liveness under SpecLive (TLC passes)      *)
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
