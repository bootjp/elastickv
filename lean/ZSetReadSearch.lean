import Std

/-!
The Redis ZSet checker explores the same enabled mutations in both versions.
This model abstracts those common ordering choices into `options`, which may
depend on the remaining operations. Each choice is required or optional and
has an arbitrary state transition. The old search advances a set of states
together; the revised search advances one state at a time. Lists model sets
extensionally through membership. The proof does not assume that a mutation
succeeds or that optional mutations execute.

This establishes the state-lifting refactor. Memoization returns the same
search result for an identical key, but the finite budget and Redis error
classification are outside this model.
-/
namespace ZSetReadSearch

variable {State : Type}

abbrev Choice (State : Type) := Nat × Bool × (State → List State)

-- `true` means required; `false` permits skipping the operation.
def oldSearch (options : List Nat → List (Choice State)) : Nat → List Nat → List State → List State
  | 0, _, states => states
  | n + 1, remaining, states =>
      (options remaining).flatMap fun (index, required, advance) =>
        let rest := remaining.erase index
        (if required then [] else oldSearch options n rest states) ++
          oldSearch options n rest (states.flatMap advance)

def newSearch (options : List Nat → List (Choice State)) : Nat → List Nat → State → List State
  | 0, _, state => [state]
  | n + 1, remaining, state =>
      (options remaining).flatMap fun (index, required, advance) =>
        let rest := remaining.erase index
        (if required then [] else newSearch options n rest state) ++
          (advance state).flatMap (newSearch options n rest)

theorem old_new_same_states (options : List Nat → List (Choice State))
    (fuel : Nat) (remaining : List Nat) (states : List State) (target : State) :
    target ∈ oldSearch options fuel remaining states ↔
      target ∈ states.flatMap (newSearch options fuel remaining) := by
  induction fuel generalizing remaining states with
  | zero => simp [oldSearch, newSearch]
  | succ n ih =>
      simp only [oldSearch, newSearch, List.mem_flatMap, List.mem_append]
      constructor
      · rintro ⟨choice, hchoice, h⟩
        rcases choice with ⟨index, required, advance⟩
        rcases h with h | h
        · by_cases hr : required
          · simp [hr] at h
          · have h' : target ∈ oldSearch options n (remaining.erase index) states := by simpa [hr] using h
            obtain ⟨state, hstate, htarget⟩ := List.mem_flatMap.mp ((ih (remaining.erase index) states).mp h')
            exact ⟨state, hstate, ⟨(index, required, advance), hchoice,
              Or.inl (by simpa [hr] using htarget)⟩⟩
        · obtain ⟨state, hstate, htarget⟩ :=
            List.mem_flatMap.mp ((ih (remaining.erase index) (states.flatMap advance)).mp h)
          obtain ⟨source, hsource, hstate'⟩ := List.mem_flatMap.mp hstate
          exact ⟨source, hsource, ⟨(index, required, advance), hchoice,
            Or.inr ⟨state, hstate', htarget⟩⟩⟩
      · rintro ⟨state, hstate, choice, hchoice, h⟩
        rcases choice with ⟨index, required, advance⟩
        rcases h with h | h
        · have hskip : target ∈ (if required then [] else oldSearch options n (remaining.erase index) states) := by
            by_cases hr : required
            · simp [hr] at h
            · have h' : target ∈ newSearch options n (remaining.erase index) state := by simpa [hr] using h
              simpa [hr] using
                (ih (remaining.erase index) states).mpr (List.mem_flatMap.mpr ⟨state, hstate, h'⟩)
          exact ⟨(index, required, advance), hchoice, Or.inl hskip⟩
        · obtain ⟨next, hnext, htarget⟩ := h
          have happly : target ∈ oldSearch options n (remaining.erase index) (states.flatMap advance) := by
            apply (ih (remaining.erase index) (states.flatMap advance)).mpr
            exact List.mem_flatMap.mpr
              ⟨next, List.mem_flatMap.mpr ⟨state, hstate, hnext⟩, htarget⟩
          exact ⟨(index, required, advance), hchoice, Or.inr happly⟩

end ZSetReadSearch
