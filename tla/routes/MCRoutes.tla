------------------------------- MODULE MCRoutes ------------------------------
(***************************************************************************)
(* Model-check instance for Routes.tla.                                    *)
(*                                                                         *)
(* Two .cfg files point at this module:                                    *)
(*   MCRoutes.cfg     — EnableSafety = TRUE  (correct design; TLC passes)  *)
(*   MCRoutes_gap.cfg — EnableSafety = FALSE (CatalogWatcher skips the     *)
(*                     monotonicity guard; TLC fails on                    *)
(*                     Routes4_NoEngineRegression with a counterexample)   *)
(*                                                                         *)
(* Run via `make tla-check` from the repository root.                      *)
(***************************************************************************)

EXTENDS Routes, TLC

\* Nodes are symmetric in Routes.tla per §6.3 of the design doc — a
\* node only matters relative to its identity; the invariants are
\* over `\A n \in Nodes`.
NodeSymmetry == Permutations(Nodes)

=============================================================================
