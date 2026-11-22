package server

import (
	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/kv"
)

func forwardTransactionForGroup(rt *clusterbootstrap.GroupRuntime, groups map[uint64]*kv.ShardGroup, observer kv.ProposalObserver) kv.Transactional {
	if group := groups[rt.Spec.ID]; group != nil {
		if tx := group.ForwardTransaction(rt.Spec.ID); kv.MigrationPartitionRequestAdmissionReady(tx) {
			return tx
		}
	}
	return adapter.NewInternalMigrationTransaction(proposerForGroup(rt, groups), observer, rt.Store, rt.Spec.ID)
}
