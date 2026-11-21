import Std

namespace ScanKeysOrder

-- List order on unsigned bytes is the lexicographic order used by bytes.Compare.
abbrev Key := List UInt8

def sortKeys (keys : List Key) : List Key :=
  keys.mergeSort (fun a b => decide (a ≤ b))

-- Model the ordering step of appendScanKeyResult after the seen check and copy.
def orderAppend (keys : List Key) (key : Key) : List Key :=
  if keys.getLast?.all (fun last => decide (last ≤ key)) then keys ++ [key]
  else sortKeys (keys ++ [key])

-- Every key preceding the last one is bounded by it. Thus a single comparison
-- suffices even when the list contains binary keys or proper prefixes.
theorem orderAppend_eq_sort (keys : List Key) (key : Key) (sorted : keys.Pairwise (· ≤ ·)) :
    orderAppend keys key = sortKeys (keys ++ [key]) := by
  unfold orderAppend
  split
  · rename_i fits
    apply Eq.symm
    apply List.mergeSort_of_sorted
    simp only [decide_eq_true_eq]
    cases h : keys.getLast? with
    | none =>
      have := List.getLast?_eq_none_iff.mp h
      simp [this]
    | some last =>
      have bound : last ≤ key := by simpa [h] using fits
      obtain ⟨init, rfl⟩ := List.getLast?_eq_some_iff.mp h
      have before := (List.pairwise_append.mp sorted).2.2
      apply List.pairwise_append.mpr
      refine ⟨sorted, List.pairwise_singleton .., ?_⟩
      intro a ha b hb
      obtain rfl : b = key := List.mem_singleton.mp hb
      rcases List.mem_append.mp ha with ha | ha
      · exact List.le_trans (before a ha last (by simp)) bound
      · obtain rfl := List.mem_singleton.mp ha
        exact bound
  · rfl

-- The trim keeps the same keys and deletes exactly the same suffix from seen.
theorem trim_eq_sort (keys : List Key) (key : Key) (limit : Nat) (sorted : keys.Pairwise (· ≤ ·)) :
    let full := sortKeys (keys ++ [key])
    ((orderAppend keys key).take limit, (orderAppend keys key).drop limit) =
      (full.take limit, full.drop limit) := by
  rw [orderAppend_eq_sort keys key sorted]

-- Trimming reestablishes the precondition for the next discovery.
theorem trimmed_sorted (keys : List Key) (key : Key) (limit : Nat) (sorted : keys.Pairwise (· ≤ ·)) :
    ((orderAppend keys key).take limit).Pairwise (· ≤ ·) := by
  rw [orderAppend_eq_sort keys key sorted]
  apply List.Pairwise.take
  have h := List.sorted_mergeSort (le := fun (a b : Key) => decide (a ≤ b))
    (by simp only [decide_eq_true_eq]; exact fun _ _ _ => List.le_trans)
    (by simpa using fun (a b : Key) => List.le_total a b)
    (keys ++ [key])
  simpa [sortKeys] using h

end ScanKeysOrder
