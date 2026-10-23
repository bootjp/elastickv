package monitoring

// Shared Prometheus label names used by more than one collector in this
// package. Keeping them in one place guarantees every metric family spells
// a given dimension identically.
const (
	labelAction        = "action"
	labelCommand       = "command"
	labelGroup         = "group"
	labelMemberAddress = "member_address"
	labelMemberID      = "member_id"
	labelMode          = "mode"
	labelOperation     = "operation"
	labelOutcome       = "outcome"
	labelQueue         = "queue"
	labelReason        = "reason"
	labelResult        = "result"
	labelTable         = "table"

	// labelValueUnknown is the shared label value for an unclassified input.
	labelValueUnknown = "unknown"
)
