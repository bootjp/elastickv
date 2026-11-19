import Std

namespace RouteHistory

-- bytes.Compare orders unsigned bytes lexicographically. A missing end denotes +infinity.
abbrev Key := List UInt8

structure Catalog where
  size : Nat
  start : Nat → Key
  stop : Nat → Option Key
  ordered : ∀ i j, i ≤ j → j < size → start i ≤ start j
  disjoint : ∀ i, i + 1 < size → ∃ e, stop i = some e ∧ e ≤ start (i+1)

def beforeEnd (stop : Option Key) (key : Key) : Bool :=
  stop.elim true (fun e => decide (key < e))

def covers (c : Catalog) (key : Key) (i : Nat) : Bool :=
  decide (c.start i ≤ key) && beforeEnd (c.stop i) key

-- sort.Search on Start > key. hi is exclusive.
def upper (start : Nat → Key) (key : Key) (lo hi : Nat) : Nat :=
  if lo < hi then
    let mid := (lo + hi) / 2
    if start mid ≤ key then upper start key (mid+1) hi
    else upper start key lo mid
  else lo
termination_by hi - lo
decreasing_by all_goals omega

private theorem partition (c : Catalog) (key : Key) (lo hi : Nat) (bounds : lo ≤ hi) (within : hi ≤ c.size)
    (left : ∀ i, i < lo → c.start i ≤ key)
    (right : ∀ i, hi ≤ i → i < c.size → key < c.start i) :
    let cut := upper c.start key lo hi
    cut ≤ c.size ∧ ∀ i, i < c.size → (c.start i ≤ key ↔ i < cut) := by
  rw [upper]
  split
  · let mid := (lo + hi) / 2
    have hm : lo ≤ mid ∧ mid < hi := by dsimp [mid]; omega
    dsimp only
    split
    · rename_i h
      apply partition c key (mid+1) hi (by omega) within _ right
      intro i ilt
      exact List.le_trans (c.ordered i mid (by omega) (by omega)) h
    · rename_i h
      apply partition c key lo mid hm.1 (by omega) left
      intro i hmi hsize
      exact Std.lt_of_lt_of_le (List.not_le.mp h) (c.ordered mid i hmi hsize)
  · refine ⟨by omega, fun i hi' => ?_⟩
    by_cases h : i < lo
    · exact ⟨fun _ => h, fun _ => left i h⟩
    · exact ⟨fun hs => False.elim ((List.not_le.mpr (right i (by omega) hi')) hs),
             fun ilt => False.elim (h ilt)⟩
termination_by hi - lo
decreasing_by all_goals omega

def lookup (c : Catalog) (key : Key) : Option Nat :=
  match upper c.start key 0 c.size with
  | 0 => none
  | i+1 => if beforeEnd (c.stop i) key then some i else none

private theorem lookup_spec (c : Catalog) (key : Key) (i : Nat) :
    lookup c key = some i ↔ i < c.size ∧ covers c key i := by
  obtain ⟨bound, part⟩ := partition c key 0 c.size (by omega) (by omega)
    (by intros; omega) (by intros; omega)
  unfold lookup
  cases h : upper c.start key 0 c.size with
  | zero =>
    rw [h] at part
    constructor
    · intro eq; cases eq
    rintro ⟨hi, hc⟩
    exfalso
    have := (part i hi).mp (of_decide_eq_true (Bool.and_eq_true_iff.mp hc).1)
    omega
  | succ last =>
    rw [h] at bound part
    have lastIn : last < c.size := by omega
    have lastStart := (part last lastIn).mpr (by omega)
    change (if beforeEnd (c.stop last) key then some last else none) = some i ↔ _
    constructor
    · intro eq
      split at eq
      · rename_i he
        cases eq
        exact ⟨lastIn, by simp [covers, lastStart, he]⟩
      · cases eq
    · rintro ⟨hi, hc⟩
      obtain ⟨startOK, endOK⟩ := Bool.and_eq_true_iff.mp hc
      have lower := (part i hi).mp (of_decide_eq_true startOK)
      have eq : i = last := by
        apply Classical.byContradiction
        intro neq
        have next := (part (i+1) (by omega)).mpr (by omega)
        obtain ⟨e, he, he'⟩ := c.disjoint i (by omega)
        simp only [beforeEnd, he, Option.elim_some, decide_eq_true_eq] at endOK
        exact (List.not_lt.mpr (List.le_trans he' next)) endOK
      subst i
      simp [endOK]

-- Equal indices give equal group IDs and every other route field. Includes
-- misses in gaps, an empty catalog, and the exclusive finite end boundary.
theorem lookup_eq_first_covering (c : Catalog) (key : Key) :
    lookup c key = (List.range c.size).find? (covers c key) := by
  cases h : (List.range c.size).find? (covers c key) with
  | some i =>
    exact (lookup_spec c key i).mpr
      ⟨List.mem_range.mp (List.mem_of_find?_eq_some h), List.find?_some h⟩
  | none =>
    cases h' : lookup c key with
    | none => rfl
    | some i =>
      obtain ⟨hi, hc⟩ := (lookup_spec c key i).mp h'
      exact False.elim ((List.find?_eq_none.mp h) i (List.mem_range.mpr hi) hc)

-- The direct comparison for the common one-route catalog specializes lookup.
theorem lookup_singleton (c : Catalog) (key : Key) (one : c.size = 1) :
    lookup c key = (if covers c key 0 then some 0 else none) := by
  rw [lookup_eq_first_covering, one]
  cases h : covers c key 0 <;> simp [List.range_succ, h]

-- A version identifies one immutable ownership map. For a fixed request
-- validate returns the first offending key, if any (including a missing owner).
-- An observation failure takes precedence over a current failure.
inductive Verdict (α : Type) where
  | ok
  | observed (key : α)
  | current (key : α)

def checkOwners (validate : Nat → Option α) (observed current : Nat) (skipSame : Bool) : Verdict α :=
  match validate observed with
  | some err => .observed err
  | none =>
    if skipSame && observed == current then .ok
    else (validate current).elim .ok Verdict.current

theorem skip_same_version_eq (validate : Nat → Option α) (observed current : Nat) :
    checkOwners validate observed current true = checkOwners validate observed current false := by
  by_cases h : observed = current
  · subst current
    cases hv : validate observed <;> simp [checkOwners, hv]
  · simp [checkOwners, h]

end RouteHistory
