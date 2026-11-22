import Std

/-! Applied-prefix and serving properties for a voter joining the target
of an online RawKV split after the final readiness confirmation. This
model uses durable Raft log matching as an explicit premise. -/

namespace SplitMembership

theorem learner_contains_ready_prefix {Entry : Type}
    (readyPrefix leaderTail learnerLog : List Entry) (sampledCommit : Nat)
    (logMatching : learnerLog = (readyPrefix ++ leaderTail).take learnerLog.length)
    (sampleIncludesReady : readyPrefix.length ≤ sampledCommit)
    (appliedGuard : sampledCommit ≤ learnerLog.length) :
    learnerLog.take readyPrefix.length = readyPrefix := by
  have covers : readyPrefix.length ≤ learnerLog.length :=
    Nat.le_trans sampleIncludesReady appliedGuard
  calc
    learnerLog.take readyPrefix.length =
        ((readyPrefix ++ leaderTail).take learnerLog.length).take readyPrefix.length :=
      congrArg (List.take readyPrefix.length) logMatching
    _ = readyPrefix := by
      rw [List.take_take, Nat.min_eq_left covers]
      simp

/-- The staged data, including a tombstone, is reconstructed by replaying
    the exact Ready prefix. Later entries are not folded into this witness. -/
theorem learner_reconstructs_sealed_image {Entry State : Type}
    (apply : State → Entry → State) (initial : State)
    (readyPrefix leaderTail learnerLog : List Entry) (sampledCommit : Nat)
    (logMatching : learnerLog = (readyPrefix ++ leaderTail).take learnerLog.length)
    (sampleIncludesReady : readyPrefix.length ≤ sampledCommit)
    (appliedGuard : sampledCommit ≤ learnerLog.length) :
    (learnerLog.take readyPrefix.length).foldl apply initial =
      readyPrefix.foldl apply initial := by
  rw [learner_contains_ready_prefix readyPrefix leaderTail learnerLog sampledCommit
    logMatching sampleIncludesReady appliedGuard]

inductive ReadResult (Value : Type) where
  | unavailable
  | ok (value : Option Value)

inductive ReadPath where
  | staleSource
  | staleForwardTarget
  | currentTarget

def afterPublication {Value : Type} (localImage : Option Value)
    (path : ReadPath) : ReadResult Value :=
  match path with
  | .staleSource | .staleForwardTarget => .unavailable
  | .currentTarget => .ok localImage

theorem successful_read_matches_seal {Value : Type} {sealed localImage actual : Option Value}
    (path : ReadPath) (guard : path = .currentTarget → localImage = sealed)
    (served : afterPublication localImage path = .ok actual) :
    actual = sealed := by
  cases path with
  | staleSource | staleForwardTarget => contradiction
  | currentTarget =>
    simp [afterPublication] at served
    exact served.symm.trans (guard rfl)

/-- Composes the promotion check and the post-publication read gate. `lookup`
    projects the sealed latest value; `none` includes an acknowledged delete. -/
theorem successful_joined_read_preserves_ack {Entry State Value : Type}
    (applyEntry : State → Entry → State) (initial : State) (lookup : State → Option Value)
    (readyPrefix leaderTail learnerLog : List Entry) (sampledCommit : Nat)
    (logMatching : learnerLog = (readyPrefix ++ leaderTail).take learnerLog.length)
    (sampleIncludesReady : readyPrefix.length ≤ sampledCommit)
    (appliedGuard : sampledCommit ≤ learnerLog.length)
    (path : ReadPath) (actual : Option Value)
    (served : afterPublication (lookup ((learnerLog.take readyPrefix.length).foldl applyEntry initial))
      path = .ok actual) :
    actual = lookup (readyPrefix.foldl applyEntry initial) := by
  apply successful_read_matches_seal path _ served
  intro _
  rw [learner_reconstructs_sealed_image applyEntry initial readyPrefix leaderTail learnerLog sampledCommit
    logMatching sampleIncludesReady appliedGuard]

#print axioms learner_contains_ready_prefix
#print axioms learner_reconstructs_sealed_image
#print axioms successful_read_matches_seal
#print axioms successful_joined_read_preserves_ack

end SplitMembership
