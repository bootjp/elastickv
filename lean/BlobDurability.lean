import Std

namespace BlobDurability

-- One result per planned member in startS3BlobReplication. The durability
-- gate requires all members for offloaded chunks. An endpoint stands for a
-- separate persistent store (deployment assumption).
structure Copy where
  node : String
  address : String
  durable : Bool
  deriving DecidableEq

def publishable (members : List Copy) : Prop :=
  2 ≤ members.length ∧ (members.map Copy.node).Nodup ∧
  (members.map Copy.address).Nodup ∧ ∀ m ∈ members, m.durable = true

-- Each completed push contributes exactly one result. Requiring a durable
-- count at least as large as the planned membership rules out any failed push.
private theorem all_durable_of_count (xs : List Copy)
    (h : xs.length ≤ (xs.filter Copy.durable).length) : ∀ m ∈ xs, m.durable = true := by
  induction xs with
  | nil => simp
  | cons x xs ih =>
    have bound := List.length_filter_le Copy.durable xs
    cases hx : x.durable with
    | false => simp [List.filter, hx] at h; omega
    | true =>
      simp [List.filter, hx] at h
      simpa [hx] using ih h

theorem counted_copies_publishable (xs : List Copy) (enough : 2 ≤ xs.length)
    (ids : (xs.map Copy.node).Nodup) (destinations : (xs.map Copy.address).Nodup)
    (count : xs.length ≤ (xs.filter Copy.durable).length) : publishable xs :=
  ⟨enough, ids, destinations, all_durable_of_count xs count⟩

theorem one_loss_leaves_copy (members : List Copy) (failed : String)
    (h : publishable members) : ∃ m ∈ members, m.address ≠ failed ∧ m.durable = true := by
  obtain ⟨size, _, distinct, stored⟩ := h
  match members with
  | [] => simp at size
  | [a] => simp at size
  | a :: b :: rest =>
    have ab : a.address ≠ b.address := by
      have := (List.nodup_cons.mp distinct).1
      simp_all
    by_cases ha : a.address = failed
    · exact ⟨b, by simp, fun hb => ab (ha.trans hb.symm), stored b (by simp)⟩
    · exact ⟨a, by simp, ha, stored a (by simp)⟩

-- Counting acknowledgements without checking destinations permits two names
-- backed by a single failed endpoint.
example :
    let members : List Copy := [⟨"n1", "p", true⟩, ⟨"n2", "p", true⟩]
    2 ≤ members.length ∧ (∀ m ∈ members, m.durable = true) ∧
      ¬ ∃ m ∈ members, m.address ≠ "p" := by decide

end BlobDurability
