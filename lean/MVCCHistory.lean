import Std

/-!
In-memory MVCC history lookup in `store/mvcc_store.go`.
Run `make lean-check` from the repository root.

`stamp i` is the unsigned commit timestamp at slice index `i`. Only indices
below the supplied length are observed. Timestamps are compared without
arithmetic, so natural-number comparisons cover the full uint64 range.
-/
namespace MVCCHistory

def Sorted (stamp : Nat → Nat) (n : Nat) : Prop :=
  ∀ i j, i ≤ j → j < n → stamp i ≤ stamp j

-- Original backwards scan: zero means absent, otherwise return index + 1.
def linearBound (stamp : Nat → Nat) (ts : Nat) : Nat → Nat
  | 0 => 0
  | n + 1 => if stamp n ≤ ts then n + 1 else linearBound stamp ts n

private theorem linear_partition (stamp : Nat → Nat) (ts n : Nat) (sorted : Sorted stamp n) :
    linearBound stamp ts n ≤ n ∧
    ∀ i, i < n → (stamp i ≤ ts ↔ i < linearBound stamp ts n) := by
  induction n with
  | zero => simp [linearBound]
  | succ n ih =>
    obtain ⟨bound, part⟩ := ih (fun i j hij hj => sorted i j hij (by omega))
    by_cases last : stamp n ≤ ts
    · simp only [linearBound, if_pos last, Nat.le_refl, true_and]
      intro i hi
      exact ⟨fun _ => hi, fun _ => Nat.le_trans (sorted i n (by omega) (by omega)) last⟩
    · simp only [linearBound, if_neg last]
      refine ⟨by omega, fun i hi => ?_⟩
      by_cases h : i < n
      · exact part i h
      · have : i = n := by omega
        subst i
        omega

-- sort.Search's [lo, hi) loop, specialized to stamp(mid) > ts.
private def binaryBound (stamp : Nat → Nat) (ts lo hi : Nat) : Nat :=
  if lo < hi then
    let mid := (lo + hi) / 2
    if stamp mid ≤ ts then binaryBound stamp ts (mid + 1) hi
    else binaryBound stamp ts lo mid
  else lo
termination_by hi - lo
decreasing_by all_goals omega

private theorem binary_eq_cut (stamp : Nat → Nat) (ts lo hi cut : Nat)
    (left : lo ≤ cut) (right : cut ≤ hi)
    (part : ∀ i, lo ≤ i → i < hi → (stamp i ≤ ts ↔ i < cut)) :
    binaryBound stamp ts lo hi = cut := by
  rw [binaryBound]
  split
  · let mid := (lo + hi) / 2
    have hm : lo ≤ mid ∧ mid < hi := by dsimp [mid]; omega
    have atMid := part mid hm.1 hm.2
    dsimp only
    split
    · rename_i eligible
      have := atMid.mp eligible
      apply binary_eq_cut stamp ts (mid + 1) hi cut (by omega) right
      exact fun i hlo hhi => part i (by omega) hhi
    · rename_i future
      have : ¬ mid < cut := fun lt => future (atMid.mpr lt)
      apply binary_eq_cut stamp ts lo mid cut left (by omega)
      exact fun i hlo hhi => part i hlo (by omega)
  · omega
termination_by hi - lo
decreasing_by all_goals omega

-- visibleValue checks two tail entries before versionUpperBound searches the prefix.
def upperBound (stamp : Nat → Nat) (ts : Nat) : Nat → Nat → Nat
  | 0, n => binaryBound stamp ts 0 n
  | _ + 1, 0 => 0
  | k + 1, n + 1 => if stamp n ≤ ts then n + 1 else upperBound stamp ts k n

theorem upperBound_eq_linear (stamp : Nat → Nat) (ts checks n : Nat) (sorted : Sorted stamp n) :
    upperBound stamp ts checks n = linearBound stamp ts n := by
  induction checks generalizing n with
  | zero =>
    obtain ⟨bound, part⟩ := linear_partition stamp ts n sorted
    exact binary_eq_cut stamp ts 0 n _ (by omega) bound (fun i _ hi => part i hi)
  | succ k ih =>
    cases n with
    | zero => rfl
    | succ n =>
      simp only [upperBound, linearBound]
      rw [ih n (fun i j hij hj => sorted i j hij (by omega))]

-- VersionExistsAtOrBefore counts expired versions and tombstones as history.
theorem first_eligible_iff_exists (stamp : Nat → Nat) (ts n : Nat) (sorted : Sorted stamp n) :
    (0 < n ∧ stamp 0 ≤ ts) ↔ 0 < linearBound stamp ts n := by
  obtain ⟨bound, part⟩ := linear_partition stamp ts n sorted
  exact ⟨fun ⟨hn, h⟩ => (part 0 hn).mp h,
    fun h => ⟨by omega, (part 0 (by omega)).mpr h⟩⟩

structure Version (α : Type) where
  stamp : Nat
  expireAt : Nat
  tombstone : Bool
  value : α

-- GetAt and scans inspect exactly the selected version, even if it is dead.
def readValue (history : Nat → Version α) (ts : Nat) : Nat → Option α
  | 0 => none
  | i + 1 =>
    let v := history i
    if v.tombstone || (v.expireAt != 0 && v.expireAt ≤ ts) then none else some v.value

theorem historical_read_eq (history : Nat → Version α) (ts n : Nat)
    (sorted : Sorted (fun i => (history i).stamp) n) :
    readValue history ts (upperBound (fun i => (history i).stamp) ts 2 n) =
    readValue history ts (linearBound (fun i => (history i).stamp) ts n) := by
  rw [upperBound_eq_linear _ _ _ _ sorted]

private theorem drop_before_eligible (history : Nat → Version α) (ts keep m : Nat)
    (eligible : (history keep).stamp ≤ ts) :
    readValue history ts (linearBound (fun i => (history i).stamp) ts (keep + (m + 1))) =
    readValue (fun i => history (keep + i)) ts
      (linearBound (fun i => (history (keep + i)).stamp) ts (m + 1)) := by
  induction m with
  | zero => simp [linearBound, eligible, readValue]
  | succ m ih =>
    change readValue history ts (if (history (keep + (m + 1))).stamp ≤ ts
      then keep + (m + 1) + 1 else linearBound _ ts (keep + (m + 1))) =
      readValue _ ts (if (history (keep + (m + 1))).stamp ≤ ts
      then m + 1 + 1 else linearBound _ ts (m + 1))
    by_cases h : (history (keep + (m + 1))).stamp ≤ ts
    · rw [if_pos h, if_pos h]
      rfl
    · rwa [if_neg h, if_neg h]

-- compactKeepIndex keeps the last version at or before the floor, or the
-- entire history if there is no such version. Every retained read sees the
-- same value across this cut, including deletion and expiration at the floor.
theorem compaction_preserves_read (history : Nat → Version α) (floor ts n : Nat)
    (sorted : Sorted (fun i => (history i).stamp) n) (retained : floor ≤ ts) :
    let stamp := fun i => (history i).stamp
    let keep := upperBound stamp floor 0 n - 1
    readValue history ts (upperBound stamp ts 2 n) =
    readValue (fun i => history (keep + i)) ts
      (upperBound (fun i => stamp (keep + i)) ts 2 (n - keep)) := by
  dsimp only
  let stamp := fun i => (history i).stamp
  let keep := binaryBound stamp floor 0 n - 1
  change readValue history ts (upperBound stamp ts 2 n) =
    readValue (fun i => history (keep + i)) ts
      (upperBound (fun i => stamp (keep + i)) ts 2 (n - keep))
  by_cases zero : keep = 0
  · simp [zero]
  obtain ⟨bound, part⟩ := linear_partition stamp floor n sorted
  have cut := upperBound_eq_linear stamp floor 0 n sorted
  change binaryBound stamp floor 0 n = linearBound stamp floor n at cut
  have before : keep < linearBound stamp floor n := by dsimp [keep] at *; omega
  have inside : keep < n := by omega
  have eligible := Nat.le_trans ((part keep inside).mpr before) retained
  have shifted : Sorted (fun i => stamp (keep + i)) (n - keep) :=
    fun i j hij hj => sorted _ _ (by omega) (by omega)
  rw [upperBound_eq_linear _ _ _ _ sorted, upperBound_eq_linear _ _ _ _ shifted]
  obtain ⟨m, hm⟩ := Nat.exists_eq_succ_of_ne_zero (show n - keep ≠ 0 by omega)
  have whole : n = keep + (m + 1) := by omega
  exact hm ▸ whole ▸ drop_before_eligible history ts keep m eligible

end MVCCHistory
