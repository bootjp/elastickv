import Lean

namespace AdapterProtocols

def maxDuration : Int := 9223372036854775807

def ttlFits (n unit : Int) := decide (0 < n ∧ 0 < unit ∧ n * unit ≤ maxDuration)

-- The persisted deadline is absolute Unix milliseconds; reopening the row
-- cannot renew it. Physical clocks on successive readers must be monotonic.
def visible (deadline : Option Int) (now : Int) : Bool := deadline.all (now < ·)

theorem expired_stays_expired (deadline : Option Int) (h : visible deadline t = false)
    (later : t ≤ u) : visible deadline u = false := by
  cases deadline with
  | none => simp [visible] at h
  | some d => simp [visible] at h ⊢; omega

def pttl (deadline : Option Int) (tick : Int) : Int :=
  match deadline with
  | none => -1
  | some d => if tick < d then d - tick else -2

theorem script_relative_pttl (positive : 0 < ms) :
    pttl (some (tick + ms)) tick = ms := by
  have live : tick < tick + ms := by omega
  simp only [pttl, if_pos live]; omega

-- Lua's VM chooses a trace. The read set here consists of the reads in
-- that trace (the Go planner must also track type/absence and collection
-- anchors). An uncaught execution error never reaches commit.
abbrev DB := Nat → Option Int
inductive Command where
  | get (k : Nat)
  | set (k : Nat) (v : Option Int)

def update (s : DB) (k : Nat) (v : Option Int) : DB :=
  fun i => if i = k then v else s i

def execute (s : DB) : List Command → List (Option Int) × DB
  | [] => ([], s)
  | .get k :: tail => let rest := execute s tail; (s k :: rest.1, rest.2)
  | .set k v :: tail => execute (update s k v) tail

def reads : List Command → List Nat
  | [] => []
  | .get k :: tail => k :: reads tail
  | .set _ _ :: tail => reads tail

def writes : List Command → List Nat
  | [] => []
  | .get _ :: tail => writes tail
  | .set k _ :: tail => k :: writes tail

theorem lua_serial_reads (ops : List Command) (s live : DB)
    (h : ∀ k ∈ reads ops, s k = live k) :
    (execute s ops).1 = (execute live ops).1 := by
  induction ops generalizing s live with
  | nil => rfl
  | cons op tail ih =>
    cases op with
    | get k =>
      simp only [execute]
      rw [h k (by simp [reads]), ih s live (fun i hi => h i (by simp [reads, hi]))]
    | set k v =>
      apply ih; intro i hi
      by_cases e : i = k
      · simp [update, e]
      · simp [update, e, h i hi]

theorem lua_overwritten_matches (ops : List Command) (s live : DB) (k : Nat)
    (h : k ∈ writes ops ∨ s k = live k) :
    (execute s ops).2 k = (execute live ops).2 k := by
  induction ops generalizing s live with
  | nil => simpa [writes] using h
  | cons op tail ih =>
    cases op with
    | get => exact ih s live h
    | set i v =>
      apply ih
      by_cases e : k = i
      · exact Or.inr (by simp [update, e])
      · simpa [writes, e, update] using h

theorem lua_untouched (ops : List Command) (s : DB) (absent : k ∉ writes ops) :
    (execute s ops).2 k = s k := by
  induction ops generalizing s with
  | nil => rfl
  | cons op tail ih =>
    cases op with
    | get => exact ih s absent
    | set i v =>
      simp [writes] at absent
      simp [execute, ih _ absent.2, update, absent.1]

-- The single committed operation installs only the staged writes onto the
-- current database. Reads from the validated snapshot include own writes.
def install (snapshot live : DB) (ops : List Command) : DB :=
  fun k => if k ∈ writes ops then (execute snapshot ops).2 k else live k

theorem lua_atomic_commit (ops : List Command) (snapshot live : DB)
    (valid : ∀ k ∈ reads ops, snapshot k = live k) :
    ((execute snapshot ops).1, install snapshot live ops) = execute live ops := by
  apply Prod.ext
  · exact lua_serial_reads _ _ _ valid
  · funext k
    by_cases h : k ∈ writes ops
    · simpa [install, h] using lua_overwritten_matches ops snapshot live k (.inl h)
    · simp [install, h, lua_untouched ops live h]

structure Receipt where
  generation : Nat
  token : Nat
  issued : Int
  deadline : Int
  deriving DecidableEq

def limit (r : Receipt) : Int := if r.issued = 0 then r.deadline else r.issued + 43200000

def change (r : Receipt) (generation token : Nat) (now seconds : Int) : Option Receipt :=
  if generation = r.generation ∧ token = r.token ∧ now < r.deadline ∧
      0 ≤ seconds ∧ seconds ≤ 43200 ∧
      (seconds = 0 ∨ now + seconds * 1000 ≤ limit r) then
    some {r with deadline := now + seconds * 1000}
  else none

theorem visibility_ack (h : change r g t now sec = some after) :
    g = r.generation ∧ t = r.token ∧ now < r.deadline ∧ after.issued = r.issued ∧
    after.deadline = now + sec * 1000 ∧ (sec = 0 ∨ after.deadline ≤ limit r) := by
  unfold change at h
  split at h
  · rename_i yes; cases h; exact ⟨yes.1, yes.2.1, yes.2.2.1, rfl, rfl, yes.2.2.2.2.2⟩
  · contradiction

def receive (generation fresh : Nat) (start delivered sec : Int) : Receipt :=
  ⟨generation, fresh, start, min (delivered + sec * 1000) (start + 43200000)⟩

theorem long_poll_bounded : (receive gen token start delivered sec).deadline ≤ start + 43200000 :=
  Int.min_le_right ..

theorem legacy_cannot_extend (r : Receipt) (old : r.issued = 0)
    (ok : change r g t now sec = some after) : after.deadline ≤ r.deadline := by
  obtain ⟨_, _, active, _, deadline, release | bound⟩ := visibility_ack ok
  · omega
  · simpa [limit, old] using bound

theorem stale_after_redelivery (r : Receipt) (newToken : Nat) (fresh : newToken ≠ r.token) :
    change {r with token := newToken, issued := n, deadline := d} r.generation r.token now sec = none := by
  simp [change, Ne.symm fresh]

end AdapterProtocols

namespace AdapterProtocols

-- Each requested ETag is checked against its descriptor in a single snapshot.
structure Part where
  num : Nat
  tag : String
  size : Nat
  version : Nat := 0
  deriving DecidableEq, Lean.ToJson

def selectParts (lookup : Nat → Option Part) (prev : Nat) : List (Nat × String) → Option (List Part)
  | [] => some []
  | (n, tag) :: tail =>
    match lookup n with
    | none => none
    | some p =>
      if prev < n ∧ n ≤ 10000 ∧ p.tag = tag ∧ (tail = [] ∨ 5242880 ≤ p.size) then
        (selectParts lookup n tail).map (p :: ·)
      else none

theorem selected_tags (lookup : Nat → Option Part) (req : List (Nat × String)) (prev : Nat)
    (h : selectParts lookup prev req = some result) :
    result.map Part.tag = req.map Prod.snd := by
  induction req generalizing prev result with
  | nil => simp [selectParts] at h; simp [← h]
  | cons x xs ih =>
    obtain ⟨n, tag⟩ := x
    cases hp : lookup n with
    | none => simp [selectParts, hp] at h
    | some p =>
      by_cases valid : prev < n ∧ n ≤ 10000 ∧ p.tag = tag ∧ (xs = [] ∨ 5242880 ≤ p.size)
      · cases ht : selectParts lookup n xs with
        | none => simp [selectParts, hp, valid, ht] at h
        | some rest =>
          simp [selectParts, hp, valid, ht] at h
          rw [← h]; simp [valid.2.2.1, ih n ht]
      · simp [selectParts, hp, valid] at h

theorem select_validated (req : List (Nat × String)) (prev : Nat)
    (snap live : Nat → Option Part) (same : ∀ k ∈ req.map Prod.fst, snap k = live k) :
    selectParts snap prev req = selectParts live prev req := by
  induction req generalizing prev with
  | nil => rfl
  | cons x xs ih =>
    obtain ⟨k, tag⟩ := x
    simp only [selectParts, same k (by simp), ih k (fun i hi => same i (by simp [hi]))]

structure Upload where
  openUpload : Bool
  parts : Nat → Option Part
  head : Option (List Part)

-- Each fresh upload ID has its own descriptors. Starting or replacing
-- an upload part cannot publish a new object head.
def beginUpload (head : Option (List Part)) : Upload := ⟨true, fun _ => none, head⟩

def uploadPart (s : Upload) (p : Part) : Option Upload :=
  if s.openUpload ∧ 0 < p.num ∧ p.num ≤ 10000 then
    some {s with parts := fun k => if k = p.num then some p else s.parts k}
  else none

theorem staged_part (ok : uploadPart s p = some next) :
    next.head = s.head ∧ next.parts p.num = some p ∧
    (∀ k, k ≠ p.num → next.parts k = s.parts k) := by
  unfold uploadPart at ok
  split at ok
  · cases ok
    exact ⟨rfl, by simp, fun k h => by simp [h]⟩
  · contradiction

theorem fresh_session (head : Option (List Part)) :
    (beginUpload head).head = head ∧ ∀ k, (beginUpload head).parts k = none := by
  exact ⟨rfl, fun _ => rfl⟩

theorem closed_cannot_stage (closed : s.openUpload = false) :
    uploadPart s p = none := by simp [uploadPart, closed]

-- The caller selects one bucket generation, object and upload ID. The
-- bucket, old head and open session are write-fenced; part descriptors
-- are read-fenced. Publication/consumption share a single Raft transaction.
def publish (s : Upload) (req : List (Nat × String)) (snap : Nat → Option Part) : Option Upload :=
  if s.openUpload && !req.isEmpty && req.length ≤ 10000 then
    (selectParts snap 0 req).map fun ps => {s with head := some ps, openUpload := false}
  else none

theorem multipart_publish (ok : publish s req snap = some final)
    (valid : ∀ k ∈ req.map Prod.fst, snap k = s.parts k) :
    ∃ ps, selectParts s.parts 0 req = some ps ∧ final.head = some ps ∧
      ps.map Part.tag = req.map Prod.snd ∧ ∀ next lookup, publish final next lookup = none := by
  unfold publish at ok
  split at ok
  · rw [select_validated _ _ _ _ valid] at ok
    cases h : selectParts s.parts 0 req with
    | none => simp [h] at ok
    | some ps =>
      simp [h] at ok; cases ok
      exact ⟨ps, rfl, rfl, selected_tags _ _ _ h, by simp [publish]⟩
  · contradiction

def abort (s : Upload) : Upload := {s with openUpload := false}

theorem aborted_upload (s : Upload) (req : List (Nat × String)) (snap : Nat → Option Part) :
    (abort s).head = s.head ∧ publish (abort s) req snap = none := by
  simp [abort, publish]

end AdapterProtocols
