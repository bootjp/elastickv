-- The extractor treats errors.Is and the transport classifier as observations.
-- These facts can overlap when a Go error wraps multiple causes.
namespace AdapterMutation.Extracted

structure Signals where
  unknown : Bool
  dispatchLost : Bool
  conflict : Bool
  locked : Bool
  fenced : Bool
  violation : Bool
  versionGCd : Bool

def Signals.evidence (s : Signals) : Evidence :=
  ⟨s.unknown || s.dispatchLost, s.conflict, s.locked, s.fenced,
   s.violation || s.versionGCd, false⟩
