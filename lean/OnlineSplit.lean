import Std

/-! Safety model for a single persistent RawKV split generation.
A version cell is a key and an MVCC timestamp. A frame contains absolute
postimages in source apply order. In particular a tombstone is a postimage.
-/

namespace OnlineSplit

structure Location (Key : Type) where
  key : Key
  timestamp : Nat
  deriving DecidableEq

inductive Cell (Value : Type) where
  | absent
  | tombstone
  | live (value : Value)

structure Postimage (Key Value : Type) where
  location : Location Key
  cell : Cell Value

abbrev Image (Key Value : Type) := Location Key → Cell Value

variable {Key Value : Type} [DecidableEq Key]

def apply (image : Image Key Value) (effect : Postimage Key Value) :
    Image Key Value :=
  fun location => if effect.location = location then effect.cell else image location

def replay (image : Image Key Value) : List (Postimage Key Value) → Image Key Value
  | [] => image
  | effect :: tail => replay (apply image effect) tail

def touches (location : Location Key) : List (Postimage Key Value) → Prop
  | [] => False
  | effect :: tail => effect.location = location ∨ touches location tail

/-- Only cells untouched by the journal need an identical base. A cell
    touched even once converges to its last source postimage, irrespective
    of the value obtained by a concurrent base iterator. -/
theorem replay_agrees_at
    (location : Location Key) (journal : List (Postimage Key Value))
    (sourceBase targetBase : Image Key Value)
    (baseAgrees : ¬ touches location journal → sourceBase location = targetBase location) :
    replay sourceBase journal location = replay targetBase journal location := by
  induction journal generalizing sourceBase targetBase with
  | nil => exact baseAgrees (by simp [touches])
  | cons effect tail ih =>
    apply ih
    intro untouched
    by_cases same : effect.location = location
    · simp [apply, same]
    · simp [apply, same]
      exact baseAgrees (by simp [touches, same, untouched])

/-- An immediately repeated same-timestamp write or delete replaces that
    version; it is not compared with the earlier value or timestamp. -/
theorem same_version_last_postimage
    (image : Image Key Value) (first last : Postimage Key Value)
    (same : first.location = last.location) :
    replay image [first, last] = apply image last := by
  funext location
  by_cases here : last.location = location
  · simp [replay, apply, here]
  · simp [replay, apply, here, same]

/-! The sequence number is one-based on the wire. List indices are zero-based,
so the next admissible frame is at index received.length. -/

inductive ImportStep {Frame : Type} (source : List Frame) :
    List Frame → List Frame → Prop where
  | append {received} (frame : Frame)
      (atIndex : source[received.length]? = some frame) :
      ImportStep source received (received ++ [frame])
  | duplicate {received} (index : Nat) (frame : Frame)
      (atIndex : received[index]? = some frame) :
      ImportStep source received received
  | restart {received} : ImportStep source received received

inductive ImportTrace {Frame : Type} (source : List Frame) : List Frame → Prop where
  | empty : ImportTrace source []
  | next {prior current} : ImportTrace source prior →
      ImportStep source prior current → ImportTrace source current

theorem import_trace_is_exact_prefix {Frame : Type} {source received : List Frame}
    (trace : ImportTrace source received) : received = source.take received.length := by
  induction trace with
  | empty => simp
  | next _ step ih =>
    cases step with
    | append frame atIndex =>
      simp [List.take_succ, atIndex, ← ih]
    | duplicate => exact ih
    | restart => exact ih

structure ReadyWitness where
  baseComplete : Bool
  sourceFenced : Bool
  sourceValid : Bool
  sourceSealed : Bool
  importsClosed : Bool
  appliedMembersAttested : Bool
  sealSequence : Nat
  targetSequence : Nat
  attestedConfiguration : Nat
  currentConfiguration : Nat

structure ReadyWitness.CopyComplete (w : ReadyWitness) : Prop where
  baseComplete : w.baseComplete = true
  fenced : w.sourceFenced = true
  valid : w.sourceValid = true
  sealed : w.sourceSealed = true
  importsClosed : w.importsClosed = true
  completeTail : w.targetSequence = w.sealSequence

structure ReadyWitness.Authorizes (w : ReadyWitness) : Prop extends w.CopyComplete where
  membersAttested : w.appliedMembersAttested = true
  currentAttestation : w.attestedConfiguration = w.currentConfiguration

/-- Each abstract frame is a list of postimages. Frame positions still count
    empty frames. An exact durable prefix at Ready therefore contains every
    sealed frame, including deletes and same-timestamp updates. -/
theorem cutover_preserves_sealed_image
    (w : ReadyWitness) (complete : w.CopyComplete)
    (frames imported : List (List (Postimage Key Value)))
    (sealEvidence : w.sealSequence = frames.length)
    (receiptCount : imported.length = w.targetSequence)
    (receipts : ImportTrace frames imported)
    (sourceBase targetBase : Image Key Value)
    (baseAgrees : ∀ location, ¬ touches location frames.flatten →
      sourceBase location = targetBase location) :
    replay targetBase imported.flatten = replay sourceBase frames.flatten := by
  have lengthMatch : imported.length = frames.length :=
    receiptCount.trans (complete.completeTail.trans sealEvidence)
  have allImported := (import_trace_is_exact_prefix receipts).trans
    (by rw [lengthMatch, List.take_length])
  rw [allImported]
  funext location
  exact (replay_agrees_at location frames.flatten sourceBase targetBase
    (baseAgrees location)).symm

theorem stale_attestation_rejected (w : ReadyWitness)
    (stale : w.attestedConfiguration ≠ w.currentConfiguration) :
    ¬ w.Authorizes := by
  intro allowed
  exact stale allowed.currentAttestation

inductive Phase where
  | capturing | invalidated | fenced | sealed | published | cleaned | rolledBack

def Published : Phase → Prop
  | .published | .cleaned => True
  | _ => False

def SourceWriteAllowed : Phase → Prop
  | .capturing | .invalidated | .rolledBack => True
  | _ => False

inductive Step : Phase → Phase → Prop where
  | invalidate : Step .capturing .invalidated
  | fence : Step .capturing .fenced
  | seal : Step .fenced .sealed
  | publish (w : ReadyWitness) (allowed : w.Authorizes) : Step .sealed .published
  | cleanup : Step .published .cleaned
  | abortCapture : Step .capturing .rolledBack
  | abortInvalidated : Step .invalidated .rolledBack
  | abortFenced : Step .fenced .rolledBack
  | abortSealed : Step .sealed .rolledBack

inductive Trace : Phase → Phase → Prop where
  | refl (p : Phase) : Trace p p
  | next {p q r : Phase} : Trace p q → Step q r → Trace p r

theorem no_dual_public_write_authority (phase : Phase)
    (source : SourceWriteAllowed phase) : ¬ Published phase := by
  cases phase <;> simp_all [SourceWriteAllowed, Published]

theorem cleanup_requires_publication {p : Phase} (step : Step p .cleaned) :
    Published p := by
  cases step
  exact True.intro

theorem publication_is_irreversible {p q : Phase}
    (trace : Trace p q) (owner : Published p) : Published q := by
  induction trace with
  | refl => exact owner
  | next _ step ih =>
    cases step <;> simp_all [Published]

def Canceled : Phase → Prop
  | .invalidated | .rolledBack => True
  | _ => False

theorem invalidated_generation_cannot_publish {p q : Phase}
    (trace : Trace p q) (canceled : Canceled p) : ¬ Published q := by
  have staysCanceled : Canceled q := by
    induction trace with
    | refl => exact canceled
    | next _ step ih =>
      cases step <;> simp_all [Canceled]
  cases q <;> simp_all [Canceled, Published]

#print axioms same_version_last_postimage
#print axioms import_trace_is_exact_prefix
#print axioms cutover_preserves_sealed_image
#print axioms stale_attestation_rejected
#print axioms no_dual_public_write_authority
#print axioms cleanup_requires_publication
#print axioms publication_is_irreversible
#print axioms invalidated_generation_cannot_publish

end OnlineSplit
