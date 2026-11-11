package adminbridge

import (
	"strings"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// convertAdminPrincipal mirrors admin.AuthPrincipal onto the
// adapter's parallel struct. Both packages keep the principal type
// independent so the adapter stays free of internal/admin
// dependencies, but the role / access-key fields are deliberately
// 1:1 — any drift is a wiring bug, not a feature.
func convertAdminPrincipal(p admin.AuthPrincipal) adapter.AdminPrincipal {
	role := adapter.AdminRoleReadOnly
	if p.Role.AllowsWrite() {
		role = adapter.AdminRoleFull
	}
	return adapter.AdminPrincipal{AccessKey: p.AccessKey, Role: role}
}

// isLeaderChurnError reports whether err looks like one of the
// transient leader sentinels the kv coordinator and adapter
// internals emit during a leadership change. The suffix set mirrors the
// closed list in kv.leaderErrorPhrases and also includes availability
// sentinels that intentionally bypass coordinator retry, such as the
// leader-proxy circuit.
//
// Phrase matching uses HasSuffix (not Contains) on the standard
// canonical strings because every kv-internal sentinel emits the
// phrase at the END of its error chain (e.g.,
// "raft engine: not leader", "dispatch failed: leader not found").
// A user-supplied string that happens to contain a leader phrase
// in the MIDDLE of an unrelated error message therefore does not
// false-positive — P2 review on PR #634 flagged the original
// strings.Contains form for misclassifying validation messages
// like "conflicting attribute type for <user-name>" when the
// name itself was "not leader".
func isLeaderChurnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, kv.ErrLeaderNotFound) ||
		errors.Is(err, kv.ErrLeaderProxyCircuitOpen) ||
		errors.Is(err, adapter.ErrNotLeader) ||
		errors.Is(err, adapter.ErrLeaderNotFound) {
		return true
	}
	msg := err.Error()
	return strings.HasSuffix(msg, "not leader") ||
		strings.HasSuffix(msg, "leader not found") ||
		strings.HasSuffix(msg, "leadership lost") ||
		strings.HasSuffix(msg, "leadership transfer in progress") ||
		strings.HasSuffix(msg, "leader proxy circuit open")
}
