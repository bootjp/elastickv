import Std

/-! One byte-range key in one Raft data group. See tla/cas/README.md for
the implementation correspondence, assumptions and TLC companion. -/
namespace SingleKeyCAS

structure Row where
  version : Nat
  value : Nat
  deriving DecidableEq, Repr

structure Catalog where
  version : Nat := 1
  owns : Bool := true
  fenced : Bool := false
  deriving DecidableEq, Repr

inductive Entry where
  | activate
  | catalog (catalog : Catalog)
  | armMigration (armed : Bool)
  | certifyMigration
  | put (row : Row)
  | cas (observed expected : Nat) (replacement : Row)
  | cleanup (cursor ceiling limit : Nat)
  deriving DecidableEq, Repr

inductive Verdict where
  | control
  | written
  | applied (version : Nat)
  | mismatch
  | timestamp
  | unavailable
  | routeFenced
  | missingCatalog
  | cleanupBlocked
  | cleaned
  deriving DecidableEq, Repr

structure Image where
  catalog : Option Catalog
  protection : Bool := false
  migrationArmed : Bool := false
  migrationCovered : Bool := false
  rows : List Row := [⟨1, 0⟩]
  issued : Nat := 1
  -- Proof history. Neither the consumed tokens nor the verdicts are stored by Go.
  consumed : List Nat := []
  verdicts : List Verdict := []
  deriving DecidableEq, Repr

def initial (authority : Bool) : Image :=
  { catalog := if authority then some {} else none }

-- Admission binds the frame to this group and its observed catalog version.
-- A broken apply precondition halts without publishing the next index.
def canApply (s : Image) : Entry → Prop
  | .cas observed _ _ =>
    s.protection = true ∧ ∃ route, s.catalog = some route ∧ observed ≤ route.version
  | _ => True

def headVersion (s : Image) : Nat :=
  (s.rows.head?.map Row.version).getD 0

def record (s : Image) (v : Verdict) : Image :=
  { s with verdicts := v :: s.verdicts }

def write (s : Image) (row : Row) : Image :=
  { s with rows := row :: s.rows, issued := row.version }

-- One command selects at most limit rows, newest first, after the cursor
-- and at or below the ceiling. Versions within a history are unique.
def cleanupRows (rows : List Row) (cursor ceiling limit : Nat) : List Row :=
  let selected := ((rows.drop cursor).filter (fun row => row.version ≤ ceiling)).take limit
  rows.filter (fun row => row ∉ selected)

-- The two optional arguments only restore the old behavior in the examples.
-- The normal transition does not consult the watcher.
def step (s : Image) (e : Entry) (oldWatcher : Option Nat := none)
    (oldCleanup : Bool := false) : Image :=
  match e with
  | .activate => record { s with protection := true } .control
  | .catalog next => record { s with catalog := s.catalog.map (fun _ => next) } .control
  | .armMigration armed => record { s with migrationArmed := armed } .control
  | .certifyMigration => record { s with migrationCovered := true } .control
  | .put row =>
    if s.issued < row.version then record (write s row) .written
    else record s .timestamp
  | .cas observed expected row =>
    if !s.protection then record s .unavailable
    else
      match s.catalog with
      | none => record s .unavailable
      | some route =>
        if !route.owns || route.fenced then record s .routeFenced
        else if s.migrationArmed || s.migrationCovered then record s .unavailable
        else if oldWatcher.any (· < observed) then record s .missingCatalog
        else if s.issued < row.version then
          if expected = 0 ∨ expected ≠ headVersion s then record s .mismatch
          else record { write s row with consumed := expected :: s.consumed } (.applied row.version)
        else record s .timestamp
  | .cleanup cursor ceiling limit =>
    if s.protection && !oldCleanup && !s.migrationCovered then record s .cleanupBlocked
    else record { s with rows := cleanupRows s.rows cursor ceiling limit } .cleaned

-- A committed log is fixed during the proof. Applying the next entry
-- publishes the image and progress together. A replay at an already
-- durable index is skipped, even if the watcher has since advanced.
structure Replica where
  applied : Nat := 0
  watcher : Nat := 1
  image : Image
  deriving DecidableEq, Repr

inductive Run (authority : Bool) (log : List Entry) : Replica → Prop where
  | start : Run authority log { image := initial authority }
  | watch {r : Replica} (h : Run authority log r) (version : Nat) (advance : r.watcher ≤ version) :
    Run authority log { r with watcher := version }
  | apply {r : Replica} {e : Entry} (h : Run authority log r) (entry : log[r.applied]? = some e)
      (ready : canApply r.image e) :
    Run authority log { r with applied := r.applied + 1, image := step r.image e }
  | restart {r : Replica} (h : Run authority log r) :
    Run authority log { r with watcher := 1 }
  | replay {r : Replica} (h : Run authority log r) (index : Nat)
      (durable : 0 < index ∧ index ≤ r.applied) : Run authority log r

def prefixImage (authority : Bool) (log : List Entry) (index : Nat) : Image :=
  (log.take index).foldl (fun s e => step s e) (initial authority)

theorem applied_prefix {authority log r} (h : Run authority log r) :
    r.applied ≤ log.length ∧ r.image = prefixImage authority log r.applied := by
  induction h with
  | start => simp [prefixImage]
  | watch _ _ _ ih => exact ih
  | restart _ ih => exact ih
  | replay _ _ _ ih => exact ih
  | @apply r e _ entry _ ih =>
    have inside := List.getElem?_eq_some_iff.mp entry
    refine ⟨Nat.succ_le_of_lt inside.1, ?_⟩
    simp [prefixImage, List.take_succ, entry, List.foldl_append, ih.2, prefixImage]

-- The images include ownership/fences, protection, all version/value
-- rows, and the verdict of every entry in the prefix. Watchers may differ.
theorem replica_agreement {authority log a b} (ha : Run authority log a) (hb : Run authority log b)
    (sameIndex : a.applied = b.applied) : a.image = b.image := by
  rw [(applied_prefix ha).2, (applied_prefix hb).2, sameIndex]

structure Safe (s : Image) : Prop where
  headBound : headVersion s ≤ s.issued
  -- Bound the whole history so that removal can never make the clock go backwards.
  rowsBound : ∀ row ∈ s.rows, row.version ≤ s.issued
  once : s.consumed.Nodup
  issuedBound : ∀ token ∈ s.consumed, token < s.issued
  unrevivable : s.migrationCovered = false → ∀ token ∈ s.consumed, token < headVersion s
  beforeActivation : s.protection = false → s.consumed = []

private theorem head_le {s : Image} (h : ∀ row ∈ s.rows, row.version ≤ s.issued) :
    headVersion s ≤ s.issued := by
  cases hs : s.rows with
  | nil => simp [headVersion, hs]
  | cons row tail => simpa [headVersion, hs] using h row (by simp [hs])

private theorem safe_initial (authority : Bool) : Safe (initial authority) := by
  constructor <;> simp [initial, headVersion]

private theorem safe_write {s : Image} (h : Safe s) {row : Row} (fresh : s.issued < row.version) :
    Safe (write s row) := by
  have rows : ∀ v ∈ (write s row).rows, v.version ≤ (write s row).issued := by
    intro v hv
    simp only [write, List.mem_cons] at hv ⊢
    rcases hv with rfl | hv
    · exact Nat.le_refl _
    · exact Nat.le_trans (h.rowsBound v hv) (Nat.le_of_lt fresh)
  refine ⟨head_le rows, rows, h.once, ?_, ?_, h.beforeActivation⟩
  · exact fun t ht => Nat.lt_trans (h.issuedBound t ht) fresh
  · exact fun _ t ht => Nat.lt_trans (h.issuedBound t ht) fresh

private theorem safe_consume {s : Image} (h : Safe s) {row : Row} {token : Nat}
    (active : s.protection = true) (eligible : s.migrationCovered = false)
    (head : token = headVersion s) (fresh : s.issued < row.version) :
    Safe { write s row with consumed := token :: s.consumed } := by
  have hw := safe_write h fresh
  have newToken : token ∉ s.consumed := by
    intro mem
    have := h.unrevivable eligible token mem
    omega
  have newer : token < row.version := by
    have := h.headBound
    omega
  refine ⟨hw.headBound, hw.rowsBound, List.nodup_cons.mpr ⟨newToken, h.once⟩, ?_, ?_, ?_⟩
  · intro t ht
    rcases List.mem_cons.mp ht with rfl | ht
    · exact newer
    · exact hw.issuedBound t ht
  · intro _ t ht
    rcases List.mem_cons.mp ht with rfl | ht
    · exact newer
    · exact hw.issuedBound t ht
  · simp [write, active]

private theorem safe_cleanup {s : Image} (h : Safe s) {c u l : Nat}
    (allowed : s.protection = false ∨ s.migrationCovered = true) :
    Safe { s with rows := cleanupRows s.rows c u l } := by
  have rows : ∀ v ∈ cleanupRows s.rows c u l, v.version ≤ s.issued := by
    intro v hv
    exact h.rowsBound v (List.mem_filter.mp hv).1
  refine ⟨head_le rows, rows, h.once, h.issuedBound, ?_, h.beforeActivation⟩
  intro eligible t ht
  rcases allowed with inactive | excluded
  · simp [h.beforeActivation inactive] at ht
  · exact False.elim (Bool.false_ne_true (eligible.symm.trans excluded))

private theorem safe_record {s : Image} (h : Safe s) (v : Verdict) : Safe (record s v) :=
  ⟨h.headBound, h.rowsBound, h.once, h.issuedBound, h.unrevivable, h.beforeActivation⟩

private theorem safe_step {s : Image} (h : Safe s) (e : Entry) : Safe (step s e) := by
  cases e <;> simp only [step]
  case activate =>
    exact ⟨h.headBound, h.rowsBound, h.once, h.issuedBound, h.unrevivable, by simp [record]⟩
  case catalog =>
    exact ⟨h.headBound, h.rowsBound, h.once, h.issuedBound, h.unrevivable, h.beforeActivation⟩
  case armMigration =>
    exact ⟨h.headBound, h.rowsBound, h.once, h.issuedBound, h.unrevivable, h.beforeActivation⟩
  case certifyMigration =>
    exact ⟨h.headBound, h.rowsBound, h.once, h.issuedBound, by simp [record], h.beforeActivation⟩
  case put =>
    split
    · exact safe_record (safe_write h ‹_›) _
    · exact safe_record h _
  case cas observed expected row =>
    split
    · exact safe_record h _
    rename_i inactive
    have active : s.protection = true := by simp at inactive; exact inactive
    split
    · exact safe_record h _
    split
    · exact safe_record h _
    split
    · exact safe_record h _
    rename_i migration
    have uncovered : s.migrationCovered = false := by
      cases hm : s.migrationCovered <;> simp_all
    simp only [Option.any_none, Bool.false_eq_true, if_false]
    split
    · rename_i fresh
      split
      · exact safe_record h _
      · rename_i mismatch
        have same : expected = headVersion s := by
          apply Classical.byContradiction
          intro neq
          exact mismatch (Or.inr neq)
        exact safe_record (safe_consume h active uncovered same fresh) _
    · exact safe_record h _
  case cleanup cursor ceiling limit =>
    split
    · exact safe_record h _
    rename_i notBlocked
    have allowed : s.protection = false ∨ s.migrationCovered = true := by
      cases hp : s.protection <;> cases hm : s.migrationCovered <;> simp_all
    exact safe_record (safe_cleanup h allowed) _

theorem reachable_safe {authority log r} (h : Run authority log r) : Safe r.image := by
  induction h with
  | start => exact safe_initial authority
  | watch _ _ _ ih => exact ih
  | apply _ _ _ ih => exact safe_step ih _
  | restart _ ih => exact ih
  | replay _ _ _ ih => exact ih

theorem token_consumed_at_most_once {authority log r} (h : Run authority log r) :
    r.image.consumed.Nodup :=
  (reachable_safe h).once

-- Even with a changed catalog or leader, no consumed token can be
-- current in a range which remains eligible for CAS.
theorem consumed_token_stays_below_head {authority log r} (h : Run authority log r)
    (eligible : r.image.migrationCovered = false) {token : Nat} (used : token ∈ r.image.consumed) :
    token < headVersion r.image :=
  (reachable_safe h).unrevivable eligible token used

theorem consumed_token_cannot_apply_again {authority log r} (h : Run authority log r)
    {token : Nat} (used : token ∈ r.image.consumed) (observed version : Nat) (row : Row) :
    (step r.image (.cas observed token row)).verdicts.head? ≠ some (.applied version) := by
  simp only [step, Option.any_none, Bool.false_eq_true, if_false]
  split
  · simp [record]
  split
  · simp [record]
  split
  · simp [record]
  split
  · simp [record]
  rename_i migration
  have uncovered : r.image.migrationCovered = false := by
    cases hm : r.image.migrationCovered <;> simp_all
  have stale := Nat.ne_of_lt (consumed_token_stays_below_head h uncovered used)
  simp [stale, record] <;> split <;> simp

theorem issued_never_decreases (s : Image) (e : Entry) : s.issued ≤ (step s e).issued := by
  cases e <;> simp only [step] <;>
    repeat (first | exact Nat.le_refl _ | exact Nat.le_of_lt (by assumption) | split)

theorem producing_version_strictly_increases (s : Image) (e : Entry) :
    s.rows.length < (step s e).rows.length → s.issued < (step s e).issued := by
  cases e <;> simp only [step]
  case cleanup cursor ceiling limit =>
    split
    · simp [record]
    · intro growth
      have bound : (cleanupRows s.rows cursor ceiling limit).length ≤ s.rows.length :=
        List.length_filter_le _ _
      exact False.elim (Nat.not_lt_of_ge bound growth)
  all_goals
    repeat (first | simp [record, write, *] | split)

private def active : Image := step (initial true) .activate
private def splitImage : Image := step active (.catalog { version := 2 })
private def first : Entry := .cas 2 1 ⟨2, 7⟩
private def retry : Entry := .cas 2 1 ⟨3, 8⟩
private def boundedCleanup : Entry := .cleanup 0 2 1

example : canApply splitImage first :=
  ⟨rfl, { version := 2 }, rfl, Nat.le_refl _⟩
example : ¬ canApply active first := by simp [active, first, canApply, step, initial, record]
example : ¬ canApply (step (initial false) .activate) first := by
  simp [first, canApply, step, initial, record]

-- Old apply skips a committed CAS on the replica behind the same-owner split
-- even though both replicas durably record the same progress.
private def oldReplica (watcher : Nat) : Replica :=
  { applied := 3, watcher, image := step splitImage first (some watcher) }

example : (oldReplica 2).applied = (oldReplica 1).applied ∧
    (oldReplica 2).image.rows ≠ (oldReplica 1).image.rows := by decide
example : (oldReplica 1).image.verdicts.head? = some .missingCatalog := by decide

-- Old cleanup deletes only version 2, exposes version 1, and consumes it twice.
example : (step (step (step splitImage first) boundedCleanup none true) retry).consumed = [1, 1] := by
  decide

example : (step (step (step splitImage first) boundedCleanup) retry).verdicts.head? = some .mismatch := by
  decide

example : (step (step splitImage (.catalog { version := 3, fenced := true })) first).verdicts.head? =
    some .routeFenced := by decide

private def armedImage : Image := step (step splitImage first) (.armMigration true)

example : (step armedImage boundedCleanup).verdicts.head? = some .cleanupBlocked := by decide

example :
    let cleaned := step (step armedImage .certifyMigration) boundedCleanup
    headVersion cleaned = 1 ∧
      (step (step cleaned (.armMigration false)) retry).verdicts.head? = some .unavailable := by
  decide

-- Compaction may discard older physical rows independently while preserving
-- the same current row. Covered ranges can still have ordinary point readers.
example :
    let kept := step armedImage .certifyMigration
    let compacted := { kept with rows := kept.rows.take 1 }
    kept.rows.head? = compacted.rows.head? ∧
      (step kept boundedCleanup).rows.head? ≠ (step compacted boundedCleanup).rows.head? := by
  decide

end SingleKeyCAS
