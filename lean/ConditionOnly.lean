import Std

namespace ConditionOnly

-- Logical item identity includes the table generation and primary key.
abbrev Key := Nat
abbrev Items (α : Type) := Key → Option α

structure Check (α : Type) where
  key : Key
  predicate : Option α → Bool

-- Report the first failed condition in request order. An absent item is a
-- predicate input too (for example, attribute_not_exists).
def evaluate (get : Items α) : List (Check α) → Option Key
  | [] => none
  | c :: cs => if c.predicate (get c.key) then evaluate get cs else some c.key

theorem evaluate_congr (a b : Items α) (cs : List (Check α))
    (agree : ∀ c ∈ cs, a c.key = b c.key) : evaluate a cs = evaluate b cs := by
  induction cs with
  | nil => rfl
  | cons c cs ih =>
    simp only [evaluate, agree c (by simp)]
    rw [ih (fun k hk => agree k (by simp [hk]))]

-- A version arriving after sealing is either for another item or has a
-- timestamp strictly above cut. Older intents must already be decided for a
-- successful snapshot read. No claim is made about a failed or partial seal.
structure Version (α : Type) where
  key : Key
  ts : Nat
  value : Option α

def atCut (cut : Nat) (items : Items α) (v : Version α) : Items α :=
  fun k => if v.key = k ∧ v.ts ≤ cut then v.value else items k

private theorem stable_item (cut k : Nat) (items : Items α) (later : List (Version α))
    (stable : ∀ v ∈ later, v.key = k → cut < v.ts) :
    (later.foldl (atCut cut) items) k = items k := by
  induction later generalizing items with
  | nil => rfl
  | cons v vs ih =>
    rw [List.foldl_cons, ih _ (fun w hw => stable w (by simp [hw]))]
    have excluded : ¬ (v.key = k ∧ v.ts ≤ cut) := by
      rintro ⟨eq, le⟩
      exact (Nat.not_lt_of_ge le) (stable v (by simp) eq)
    simp [atCut, excluded]

-- Each predicate may execute at a different time and see a different prefix
-- of later arrivals. Every successful read still observes the same MVCC cut.
def observed (cut : Nat) (items : Items α) (arrivals : Key → List (Version α)) : Items α :=
  fun k => ((arrivals k).foldl (atCut cut) items) k

theorem condition_results_at_sealed_snapshot (cut : Nat) (items : Items α)
    (arrivals : Key → List (Version α)) (cs : List (Check α))
    (sealedKeys : ∀ c ∈ cs, ∀ v ∈ arrivals c.key, v.key = c.key → cut < v.ts) :
    evaluate (observed cut items arrivals) cs = evaluate items cs :=
  evaluate_congr _ _ _ (fun c hc => stable_item cut c.key items _ (sealedKeys c hc))

-- For a matching version still able to arrive after a successful lock probe,
-- a pre-seal prepare would have left an unresolved intent. Otherwise OCC
-- accepted its read of the seal, hence its start is at least the seal time.
theorem condition_results_after_intent_probe (cut : Nat) (items : Items α)
    (arrivals : Key → List (Version α)) (cs : List (Check α))
    (start : Version α → Nat) (preparedBefore : Version α → Prop)
    (noPending : ∀ c ∈ cs, ∀ v ∈ arrivals c.key, v.key = c.key → ¬ preparedBefore v)
    (occ : ∀ c ∈ cs, ∀ v ∈ arrivals c.key,
      v.key = c.key → ¬ preparedBefore v → cut ≤ start v)
    (ordered : ∀ c ∈ cs, ∀ v ∈ arrivals c.key, start v < v.ts) :
    evaluate (observed cut items arrivals) cs = evaluate items cs := by
  apply condition_results_at_sealed_snapshot
  intro c hc v hv same
  exact Nat.lt_of_le_of_lt (occ c hc v hv same (noPending c hc v hv same)) (ordered c hc v hv)

-- The previous physical plan issued a same-value Put for a present item
-- and no mutation for an absent one. Ignore MVCC version numbers here:
-- avoiding an extra version at commit is an intended observable difference.
def writeBack (snapshot : Items α) (current : Items α) (key : Key) : Items α :=
  match snapshot key with
  | none => current
  | some value => fun k => if k = key then some value else current k

theorem omit_validated_writebacks (snapshot current : Items α) (keys : List Key)
    (valid : ∀ k ∈ keys, current k = snapshot k) :
    keys.foldl (writeBack snapshot) current = current := by
  induction keys with
  | nil => rfl
  | cons k ks ih =>
    have id : writeBack snapshot current k = current := by
      funext q
      cases h : snapshot k with
      | none => simp [writeBack, h]
      | some value =>
        by_cases eq : q = k
        · subst q
          simp [writeBack, h, valid k (by simp)]
        · simp [writeBack, h, eq]
    rw [List.foldl_cons, id, ih (fun q hq => valid q (by simp [hq]))]

end ConditionOnly
