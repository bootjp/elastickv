import Std

/-!
Single-key RawKV acknowledgement model.

`apply` represents a durable Raft apply in log order. A successful write
response may be sent only for an applied entry. A latest read must fence at
least that entry before sampling the version. The model deliberately does not
claim that the Go implementation satisfies these premises. The public RawKV
regression test covers an invalid zero-index receipt, which is one necessary
condition for a successful acknowledgement.
-/
namespace RawAck

structure Register where
  index : Nat
  stamp : Nat
  value : Nat
  deriving Repr

def apply (s : Register) (index stamp value : Nat) : Option Register :=
  if s.index < index && s.stamp < stamp then
    some ⟨index, stamp, value⟩
  else
    none

def latestRead (s : Register) (fence : Nat) : Option Nat :=
  if s.index ≤ fence then some s.value else none

theorem acknowledged_write_visible (s next : Register) (index stamp value fence : Nat)
    (applied : apply s index stamp value = some next)
    (fenced : index ≤ fence) : latestRead next fence = some value := by
  unfold apply at applied
  split at applied
  · cases applied
    simp [latestRead, fenced]
  · simp at applied

theorem stale_write_cannot_be_acknowledged (s : Register) (index stamp value : Nat)
    (stale : stamp ≤ s.stamp) : apply s index stamp value = none := by
  simp [apply, Nat.not_lt.mpr stale]

theorem zero_index_cannot_be_acknowledged (s : Register) (stamp value : Nat) :
    apply s 0 stamp value = none := by
  simp [apply]

theorem unfenced_read_cannot_confirm (s next : Register) (index stamp value fence : Nat)
    (applied : apply s index stamp value = some next)
    (unfenced : fence < index) : latestRead next fence = none := by
  unfold apply at applied
  split at applied
  · cases applied
    simp [latestRead, Nat.not_le.mpr unfenced]
  · simp at applied

-- A nonzero index alone cannot establish that a write was applied. This
-- counterexample has a nonzero receipt but retains the previous value.
example : 11 ≠ 0 ∧ latestRead ⟨10, 10, 10⟩ 11 = some 10 := by decide

end RawAck
