import Lean

namespace AdapterMutation

-- Observations at the dispatch boundary. Independent flags model joined Go
-- errors: a conflict may be present at the same time as a lost response.
structure Evidence where
  uncertain : Bool
  conflict : Bool
  locked : Bool
  fenced : Bool
  composed : Bool
  circuitOpen : Bool
  deriving Lean.ToJson

inductive Policy where
  | redis | aws
  deriving DecidableEq

-- aws is the common retry decision for DynamoDB, S3 metadata and SQS.
def retry (p : Policy) (e : Evidence) : Bool :=
  !e.uncertain && (e.conflict || e.locked || e.fenced || (match p with | .redis => e.composed | .aws => false))

def skipReceiveRace (e : Evidence) : Bool :=
  !e.uncertain && !e.fenced && (e.conflict || e.locked)

inductive GRPCFailure where
  | indeterminate | notAdmitted | aborted
  deriving DecidableEq, Repr

-- Projection of unmarked dispatch errors. Isolated coordinator witnesses
-- use their own public evidence path. A secondary or joined cause is mixed.
def grpcFailure (e : Evidence) (mixed : Bool := false) : GRPCFailure :=
  if e.uncertain || mixed || e.fenced || e.locked then .indeterminate
  else if e.circuitOpen then .notAdmitted
  else if e.conflict then .aborted
  else .indeterminate

theorem uncertain_stops (p : Policy) (e : Evidence) (h : e.uncertain = true) :
    retry p e = false ∧ skipReceiveRace e = false ∧ grpcFailure e = .indeterminate := by
  simp [retry, skipReceiveRace, grpcFailure, h]

theorem grpc_aborted_requires_conflict (e : Evidence) (h : grpcFailure e = .aborted) :
    e.uncertain = false ∧ e.fenced = false ∧ e.locked = false ∧
    e.circuitOpen = false ∧ e.conflict = true := by
  simp [grpcFailure] at h
  split at h <;> simp_all
  split at h <;> simp_all

-- Trace of dispatch attempts when supplied the outcomes in order. An earlier
-- stop (budget, cancellation) can only shorten this trace.
def attempts (p : Policy) : List Evidence → List Evidence
  | [] => []
  | e :: tail => e :: (if retry p e then attempts p tail else [])

theorem no_dispatch_after_uncertainty (p : Policy) (before after : List Evidence)
    (e : Evidence) (h : e.uncertain = true) :
    attempts p (before ++ e :: after) = attempts p (before ++ [e]) := by
  induction before with
  | nil => simp [attempts, (uncertain_stops p e h).1]
  | cons head rest ih => simp [attempts, ih]

-- The Go conformance test feeds real error chains to the production helpers
-- and checks their decisions against these same executable definitions.
structure Case where
  evidence : Evidence
  retryRedis : Bool
  retryAWS : Bool
  skipRace : Bool
  grpcReason : String
  grpcMixedReason : String
  deriving Lean.ToJson

private def GRPCFailure.reason : GRPCFailure → String
  | .indeterminate => "INDETERMINATE"
  | .notAdmitted => "NOT_ADMITTED"
  | .aborted => "PROVEN_NONCOMMIT"

def cases : List Case := List.ofFn fun (i : Fin 64) =>
  let bit := i.val.testBit
  let e := Evidence.mk (bit 0) (bit 1) (bit 2) (bit 3) (bit 4) (bit 5)
  ⟨e, retry .redis e, retry .aws e, skipReceiveRace e,
    (grpcFailure e).reason, (grpcFailure e true).reason⟩

end AdapterMutation

def main : IO Unit := IO.println (Lean.toJson AdapterMutation.cases).compress
