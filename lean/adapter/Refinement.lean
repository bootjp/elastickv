
-- These names refer to definitions translated from the checked-out Go bodies.
theorem retry_refines (s : Signals) :
    isRetryableRedisTxnErr s = retry .redis s.evidence ∧
    isRetryableTransactWriteError s = retry .aws s.evidence ∧
    isRetryableS3MutationErr s = retry .aws s.evidence := by
  simp only [isRetryableRedisTxnErr, isRedisIndeterminate, isReusableRedisTxnErr,
    isRedisComposedRouteErr, isRouteWriteFencedError, isRetryableTransactWriteError,
    isRetryableS3MutationErr, retry, Signals.evidence]
  bv_decide

theorem lost_dispatch_cannot_reuse_exec (s : Signals) (h : s.evidence.uncertain = true) :
    isAmbiguousRedisExecDispatchErr s = false := by
  change (s.unknown || s.dispatchLost) = true at h
  simp [isAmbiguousRedisExecDispatchErr, isRedisIndeterminate, h]

end AdapterMutation.Extracted
