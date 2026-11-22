import Std

namespace FuseMutationBarrier

-- One inode, at the mutex boundaries of mutationTracker. sticky is observable
-- through begin/wait. Counts model uint64 provided an increment does not wrap.
structure State where
  unknown : Nat
  sticky : Bool

inductive Event where
  | markUnknown | resolveUnknown | finishOrdinary

def step (s : State) : Event → State
  | .markUnknown => ⟨s.unknown + 1, true⟩
  | .resolveUnknown =>
    match s.unknown with
    | 0 => s
    | n + 1 => ⟨n, n != 0⟩
  | .finishOrdinary => s

-- Reference semantics counts outstanding decisions independently of sticky.
def outstanding (n : Nat) : Event → Nat
  | .markUnknown => n + 1
  | .resolveUnknown => n - 1
  | .finishOrdinary => n

def represents (s : State) (n : Nat) : Prop :=
  s.unknown = n ∧ s.sticky = (n != 0)

theorem step_represents (s : State) (n : Nat) (e : Event) (h : represents s n) :
    represents (step s e) (outstanding n e) := by
  obtain ⟨rfl, hs⟩ := h
  cases e <;> cases hn : s.unknown <;> simp_all [represents, step, outstanding]

theorem trace_represents (events : List Event) (s : State) (n : Nat) (h : represents s n) :
    represents (events.foldl step s) (events.foldl outstanding n) := by
  induction events generalizing s n with
  | nil => exact h
  | cons e es ih => exact ih _ _ (step_represents s n e h)

theorem unresolved_blocks_barrier (events : List Event) (h : events.foldl outstanding 0 ≠ 0) :
    (events.foldl step ⟨0, false⟩).sticky = true := by
  have inv := trace_represents events ⟨0, false⟩ 0 (by simp [represents])
  rw [inv.2]
  exact bne_iff_ne.mpr h

end FuseMutationBarrier
