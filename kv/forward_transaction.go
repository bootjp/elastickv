package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
)

// ForwardTransaction borrows the group's local commit queue for Internal.Forward.
// The receiver must already have verified leadership and stamped/validated the
// requests, including timestamp admission. It must not restamp those epochs or
// forward them again. The group owns the manager lifetime; this port cannot Close.
// Missing/custom transactions return nil so callers retain their existing path.
func (g *ShardGroup) ForwardTransaction(groupID uint64) Transactional {
	if g == nil {
		return nil
	}
	manager := g.localGroupTransactionManager(g.Txn)
	if manager == nil {
		return nil
	}
	admission := manager.migrationAdmission.Load()
	if admission == nil || admission.groupID != groupID {
		return nil
	}
	return &forwardedGroupTransaction{manager: manager}
}

func (g *ShardGroup) localGroupTransactionManager(tx Transactional) *TransactionManager {
	switch t := tx.(type) {
	case *LeaderProxy:
		if t != nil && t.group == g {
			return t.tm
		}
	case *leaseRefreshingTxn:
		if t != nil {
			return g.localGroupTransactionManager(t.inner)
		}
	}
	return nil
}

type forwardedGroupTransaction struct {
	manager *TransactionManager
}

func (t *forwardedGroupTransaction) configuredManager() *TransactionManager {
	if t == nil {
		return nil
	}
	return t.manager
}

func (t *forwardedGroupTransaction) Commit(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	return t.manager.commitWithTimestampAdmission(ctx, reqs, nil)
}

func (t *forwardedGroupTransaction) Abort(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	return t.manager.Abort(ctx, reqs)
}
