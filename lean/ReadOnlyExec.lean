import Std

namespace ReadOnlyExec

-- Model the boundary after applying the queue at its snapshot. Each pending
-- builder input may expand to zero or more mutations, possibly keyed by ts.
structure Staged (μ : Type) where
  built : List μ
  pending : List (Nat → List μ)

def materialize (s : Staged μ) (ts : Nat) : List μ :=
  s.built ++ s.pending.flatMap (fun build => build ts)

inductive Event (μ : Type) where
  | allocate (ts : Nat)
  | commit (ts : Nat) (writes : List μ)

-- checked carries the snapshot reply or a validation error. The caller
-- dispatches only a validated, nonempty write set.
def finish (s : Staged μ) (ts : Nat) (checked : Except ε ρ) : Except ε ρ × List (Event μ) :=
  match checked, materialize s ts with
  | .ok reply, m :: ms => (.ok reply, [.commit ts (m :: ms)])
  | _, _ => (checked, [])

def eager (s : Staged μ) (ts : Nat) (checked : Except ε ρ) : Except ε ρ × List (Event μ) :=
  let (reply, events) := finish s ts checked
  (reply, .allocate ts :: events)

def prepare (s : Staged μ) (ts : Nat) (checked : Except ε ρ) : Except ε ρ × List (Event μ) :=
  if s.built.isEmpty && s.pending.isEmpty then finish s 0 checked
  else eager s ts checked

-- For every timestamp the eager allocator could supply, dropping its
-- allocation preserves the checked return and both commit traces are empty.
theorem empty_staged_effects (s : Staged μ) (ts : Nat) (checked : Except ε ρ)
    (hb : s.built = []) (hp : s.pending = []) :
    prepare s ts checked = (checked, []) ∧
    eager s ts checked = (checked, [.allocate ts]) := by
  cases checked <;> simp [prepare, eager, finish, materialize, hb, hp]

-- For all staged shapes the only possible trace difference is the allocation.
-- This includes a pending builder that ends up emitting no mutations.
theorem prepare_preserves_return_and_commits (s : Staged μ) (ts : Nat) (checked : Except ε ρ) :
    (prepare s ts checked).1 = (eager s ts checked).1 ∧
    (prepare s ts checked).2.filter (fun | .commit .. => true | .allocate _ => false) =
    (eager s ts checked).2.filter (fun | .commit .. => true | .allocate _ => false) := by
  by_cases hb : s.built = [] <;> by_cases hp : s.pending = []
  · obtain ⟨opt, old⟩ := empty_staged_effects s ts checked hb hp
    simp [opt, old]
  all_goals simp [prepare, hb, hp]

end ReadOnlyExec
