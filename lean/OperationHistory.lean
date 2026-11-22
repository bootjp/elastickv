import Lean

namespace OperationHistory

-- Each entry is one indivisible workload operation. A queue observation tracks
-- the lifetime of a message; a receive may legitimately return no messages.
structure Action where
  kind : String
  key : String
  value : List Int
  deriving Lean.FromJson

abbrev State := Std.TreeMap String (List Int)

def applyAction (s : State) (a : Action) : Option State := do
  let old := s.getD a.key []
  match a.kind with
  | "write" => return s.insert a.key a.value
  | "read" => guard (old == a.value); return s
  | "append" => return s.insert a.key (old ++ a.value)
  | "send" => guard old.isEmpty; return s.insert a.key [1]
  | "deliver" => guard (old == [1] || old == [2]); return s.insert a.key [2]
  | "delete" => guard (old == [2]); return s.insert a.key [3]
  | _ => none

def applyEntry (s : State) (actions : List Action) := actions.foldlM applyAction s

-- Reconstructing from a complete checkpoint and the committed suffix is
-- observationally equivalent to executing the entire log, including appends.
def replay (s : State) (log : List (List Action)) := log.foldlM applyEntry s

theorem recover_checkpoint (s checkpoint : State) (pre suffix : List (List Action))
    (h : replay s pre = some checkpoint) :
    replay s (pre ++ suffix) = replay checkpoint suffix := by
  simp [replay, List.foldlM_append] at h ⊢
  simp [h]

structure Call where
  start : Nat
  stop : Nat
  status : String
  actions : List Action
  -- A missing rank omits the call from the chosen completion of the history.
  rank : Option Nat
  deriving Lean.FromJson

def admissible (c : Call) : Bool :=
  c.start < c.stop && match c.status with
    | "ok" => c.rank.isSome
    | "fail" => c.rank.isNone
    | "info" => true
    | _ => false

-- A lost reply leaves an operation pending. Known effects observed inside
-- a failed batch receive have their own bounded observation in the export.
def bounded (c : Call) (point : Nat) : Bool :=
  c.status == "info" || point ≤ c.stop

inductive Linearization : Nat → State → List Call → State → Prop where
  | nil : Linearization floor s [] s
  | cons (starts : c.start ≤ point) (ordered : floor ≤ point)
      (ends : bounded c point = true) (effect : applyEntry s c.actions = some next)
      (tail : Linearization point next cs last) : Linearization floor s (c :: cs) last

def checkOrder (floor : Nat) (s : State) : List Call → Option State
  | [] => some s
  | c :: cs => do
    let point := max floor c.start
    if bounded c point then
      let next ← applyEntry s c.actions
      checkOrder point next cs
    else none

theorem checkOrder_sound (cs : List Call) (floor : Nat) (s last : State)
    (h : checkOrder floor s cs = some last) : Linearization floor s cs last := by
  induction cs generalizing floor s with
  | nil => simp [checkOrder] at h; subst last; exact .nil
  | cons c cs ih =>
    cases hb : bounded c (max floor c.start) with
    | false => simp [checkOrder, hb] at h
    | true =>
      cases he : applyEntry s c.actions with
      | none => simp [checkOrder, hb, he] at h
      | some next =>
        simp [checkOrder, hb, he] at h
        exact .cons (Nat.le_max_right ..) (Nat.le_max_left ..) hb he (ih _ _ h)

-- A completed call later in the witness cannot have ended before its floor.
theorem completed_after_floor (run : Linearization floor s cs last)
    (mem : c ∈ cs) (known : c.status ≠ "info") : floor ≤ c.stop := by
  induction run with
  | nil => simp at mem
  | cons starts ord ends effect tail ih =>
    rcases List.mem_cons.mp mem with eq | later
    · subst c
      simp [bounded, known] at ends
      omega
    · exact Nat.le_trans ord (ih later)

-- Reversing a completed-before-invoked pair is impossible in an accepted run.
theorem respects_real_time (run : Linearization floor s (a :: tail) last)
    (mem : b ∈ tail) (known : b.status ≠ "info") : a.start ≤ b.stop := by
  cases run with
  | cons starts _ _ _ rest => exact Nat.le_trans starts (completed_after_floor rest mem known)

theorem replay_witness (run : Linearization floor s cs last) :
    replay s (cs.map Call.actions) = some last := by
  induction run with
  | nil => rfl
  | cons _ _ _ applied _ ih =>
    simpa [replay, List.foldlM_cons, applied] using ih

structure Delivery where
  group : String
  sendStart : Nat
  sendEnd : Option Nat
  receivedAt : Nat
  batchIndex : Nat
  deriving Lean.FromJson

-- Within a FIFO group a later raw delivery cannot belong to a send that
-- completed before the earlier delivery's send was even invoked.
def fifoOrdered (a b : Delivery) : Bool :=
  (a.receivedAt < b.receivedAt || (a.receivedAt == b.receivedAt && a.batchIndex < b.batchIndex)) &&
    (a.group != b.group || b.sendEnd.all (a.sendStart ≤ ·))

structure Certificate where
  initial : List (String × List Int)
  calls : List Call
  deliveries : List Delivery
  deriving Lean.FromJson

def selected (cert : Certificate) : List Call :=
  (cert.calls.filter (·.rank.isSome)).mergeSort (fun a b => a.rank.getD 0 ≤ b.rank.getD 0)

def verify (cert : Certificate) : Option State := do
  if cert.calls.all admissible && decide (cert.deliveries.Pairwise fun a b => fifoOrdered a b = true) then checkOrder 0 (.ofList cert.initial) (selected cert) else none

-- Sorting changes only the order. Every success participates once per input
-- occurrence, even if two clients' operations have identical payloads.
theorem selected_perm (cert : Certificate) :
    (selected cert).Perm (cert.calls.filter (·.rank.isSome)) :=
  List.mergeSort_perm ..

theorem verified_linearization (cert : Certificate) (last : State)
    (h : verify cert = some last) :
    (∀ c ∈ cert.calls, admissible c = true) ∧
    Linearization 0 (.ofList cert.initial) (selected cert) last := by
  unfold verify at h
  split at h
  · rename_i guards
    exact ⟨List.all_eq_true.mp (Bool.and_eq_true_iff.mp guards).1, checkOrder_sound _ _ _ _ h⟩
  · contradiction

theorem verified_fifo_delivery_order (cert : Certificate) (last : State)
    (h : verify cert = some last) : cert.deliveries.Pairwise (fun a b => fifoOrdered a b = true) := by
  unfold verify at h
  split at h
  · rename_i guards
    exact of_decide_eq_true (Bool.and_eq_true_iff.mp guards).2
  · contradiction

-- Any complete checkpoint boundary in the accepted execution can be used
-- after restart. The volatile tail may be discarded; the committed suffix
-- must be present on the recovery medium. Each transaction is one log entry.
theorem recovery_of_verified_execution (cert : Certificate) (last : State)
    (ok : verify cert = some last) (pre suffix : List Call)
    (cut : selected cert = pre ++ suffix) :
    ∃ checkpoint, replay (.ofList cert.initial) (pre.map Call.actions) = some checkpoint ∧
      replay checkpoint (suffix.map Call.actions) = some last := by
  have run := replay_witness (verified_linearization cert last ok).2
  rw [cut, List.map_append] at run
  cases h : replay (.ofList cert.initial) (pre.map Call.actions) with
  | none => simp [replay, List.foldlM_append] at h run; simp [h] at run
  | some checkpoint => exact ⟨checkpoint, rfl, (recover_checkpoint _ _ _ _ h) ▸ run⟩

-- This is a valid historical response after a second completed Put. Its
-- current-value interpretation has no real-time linearization: read-before-
-- write violates the intervals, and write-before-read returns the new value.
private def replace : Call := ⟨1, 2, "ok", [⟨"write", "k", [2]⟩], some 0⟩
private def readOld : Call := ⟨3, 4, "ok", [⟨"read", "k", [1]⟩], some 1⟩

example : (checkOrder 0 (.ofList [("k", [(1 : Int)])]) [replace, readOld]).isNone = true ∧
    (checkOrder 0 (.ofList [("k", [(1 : Int)])]) [readOld, replace]).isNone = true := by decide

example : admissible { replace with rank := none } = false ∧
    admissible { replace with status := "fail" } = false := by decide

end OperationHistory


def main (args : List String) : IO Unit := do
  let [path] := args | throw (IO.userError "expected one operation certificate")
  let data ← IO.ofExcept (Lean.Json.parse (← IO.FS.readFile path))
  let cert : OperationHistory.Certificate ← IO.ofExcept (Lean.fromJson? data)
  unless (OperationHistory.verify cert).isSome do
    throw (IO.userError "invalid interval, missing required call, or illegal sequential result")
  IO.println s!"verified {cert.calls.length} calls; linearized {(OperationHistory.selected cert).length}"
