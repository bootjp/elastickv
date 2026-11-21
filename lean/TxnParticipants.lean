import Std

namespace TxnParticipants

/- The input is the sequence of admitted point mutations. Each tag is the
effective group selected by groupMutations (including an explicit group pin).
A failed write lookup or expected-group check returns before this decision.
Read owners are optional because failure to resolve a read cannot certify 1PC.
Group IDs here denote data groups (zero is reserved in the catalog). Staged
aliases are added to the same group's entry after this decision. -/
structure Write (Mutation : Type) where
  owner : Nat
  mutation : Mutation

-- One key in the write-group map means exactly one occupied bucket. Sorting
-- its keys affects PREPARE order but cannot change the singleton decision.
def occupies (writes : List (Write μ)) (g : Nat) : Prop :=
  ∃ w ∈ writes, w.owner = g

def singleOwner (writes : List (Write μ)) (reads : List (Option Nat)) : Option Nat :=
  match writes with
  | [] => none
  | first :: rest =>
    if (∀ w ∈ rest, w.owner = first.owner) ∧ (∀ r ∈ reads, r = some first.owner)
    then some first.owner else none

theorem singleOwner_iff (writes : List (Write μ)) (reads : List (Option Nat)) (g : Nat) :
    singleOwner writes reads = some g ↔
      (∀ h, occupies writes h ↔ h = g) ∧ (∀ r ∈ reads, r = some g) := by
  cases writes with
  | nil =>
    constructor
    · intro h; cases h
    · intro ⟨owners, _⟩
      obtain ⟨_, mem, _⟩ := (owners g).mpr rfl
      cases mem
  | cons first rest =>
    change (if (∀ w ∈ rest, w.owner = first.owner) ∧ (∀ r ∈ reads, r = some first.owner)
      then some first.owner else none) = some g ↔ _
    constructor
    · intro eq
      split at eq
      · rename_i all
        cases eq
        refine ⟨fun h => ⟨?_, ?_⟩, all.2⟩
        · rintro ⟨w, mem, rfl⟩
          cases mem with
          | head => rfl
          | tail _ tail => exact all.1 _ tail
        · rintro rfl
          exact ⟨first, .head _, rfl⟩
      · cases eq
    · rintro ⟨owners, readers⟩
      have head := (owners first.owner).mp ⟨first, .head _, rfl⟩
      subst g
      apply if_pos
      exact ⟨fun w mem => (owners w.owner).mp ⟨w, .tail first mem, rfl⟩, readers⟩

-- groupMutations appends in encounter order. Here a bucket is written as an
-- order-preserving selection, without hashing the payload or deduplicating it.
def bucket (g : Nat) : List (Write μ) → List μ
  | [] => []
  | w :: rest =>
    if w.owner = g then w.mutation :: bucket g rest else bucket g rest

private theorem bucket_all (writes : List (Write μ)) (g : Nat)
    (owners : ∀ w ∈ writes, w.owner = g) : bucket g writes = writes.map Write.mutation := by
  induction writes with
  | nil => rfl
  | cons first rest ih =>
    unfold bucket
    rw [if_pos (owners first (.head _))]
    exact congrArg (first.mutation :: ·) (ih fun w mem => owners w (.tail first mem))

-- Equality of lists preserves payloads, repeated keys and their order.
theorem grouped_payload_eq (writes : List (Write μ)) (reads : List (Option Nat)) (g : Nat)
    (eligible : singleOwner writes reads = some g) :
    bucket g writes = writes.map Write.mutation := by
  have owners := ((singleOwner_iff writes reads g).mp eligible).1
  exact bucket_all writes g fun w mem => (owners w.owner).mp ⟨w, mem, rfl⟩

/- Counterexample to validating a remote read first and then issuing local
1PC with no reservation. x and y are on different Raft groups and initially
zero. X writes x=1 iff it reads y=0. Y writes y=1 iff it reads x=0.
Each validation may be linearizable. A successful validation is remembered
while the other transaction proceeds. Neither local write key has changed
since its own snapshot when it applies. -/
structure State where
  x : Nat := 0
  y : Nat := 0
  checkedX : Bool := false
  checkedY : Bool := false
deriving DecidableEq

def checkX (s : State) := { s with checkedX := s.y == 0 }
def checkY (s : State) := { s with checkedY := s.x == 0 }
def applyX (s : State) := if s.checkedX && s.x == 0 then { s with x := 1 } else s
def applyY (s : State) := if s.checkedY && s.y == 0 then { s with y := 1 } else s

private def initial : State := {}
private def values (s : State) := (s.x, s.y)

theorem remote_precheck_write_skew :
    singleOwner [⟨1, ()⟩] [some 2] = none ∧ singleOwner [⟨2, ()⟩] [some 1] = none ∧
    values (applyY (applyX (checkY (checkX initial)))) = (1, 1) ∧
    values (applyY (checkY (applyX (checkX initial)))) = (1, 0) ∧
    values (applyX (checkX (applyY (checkY initial)))) = (0, 1) := by
  decide

end TxnParticipants
