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
\*
\* Symmetry must be supplied to TLC as a *group* of permutations,
\* closed under composition.  Taking the set-theoretic union of the
\* three per-domain permutation groups is NOT closed under composition
\* (a permutation that touches both Keys and Groups simultaneously is
\* not in the union) and would violate TLC's symmetry-reduction
\* assumption, silently eliding states (gemini HIGH on PR #865).
\*
\* The correct construction is the direct product: build a single
\* permutation function on Keys \cup Groups \cup TxnIds by merging the
\* three independent permutations via the TLC `@@` operator (function
\* override over disjoint domains is just function union).  Since the
\* three domains are disjoint, the result is a bijection on the union
\* and the family is closed under composition.
KeySymmetry   == Permutations(Keys)
GroupSymmetry == Permutations(Groups)
TxnSymmetry   == Permutations(TxnIds)
Symmetry      == { kSym @@ gSym @@ tSym :
                       kSym \in KeySymmetry,
                       gSym \in GroupSymmetry,
                       tSym \in TxnSymmetry }

=============================================================================
