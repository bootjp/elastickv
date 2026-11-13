package adminbridge

import (
	stderrors "errors"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/bootjp/elastickv/internal/raftengine"
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

// isLeaderChurnError accepts only typed local leadership failures. A wire
// message cannot prove that a submitted write was rejected before commit.
func isLeaderChurnError(err error) bool {
	if err == nil || errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return false
	}
	return errors.Is(err, kv.ErrLeaderNotFound) ||
		stderrors.Is(err, kv.ErrLeaderProxyCircuitOpen) ||
		errors.Is(err, adapter.ErrNotLeader) ||
		errors.Is(err, adapter.ErrLeaderNotFound) ||
		errors.Is(err, raftengine.ErrNotLeader) ||
		errors.Is(err, raftengine.ErrLeadershipTransferInProgress)
}

func isUnknownAdminOutcome(err error) bool {
	return errors.Is(err, kv.ErrTransactionOutcomeUnknown)
}

func passThroughAdminError(err error) bool {
	return err == nil || isUnknownAdminOutcome(err)
}
