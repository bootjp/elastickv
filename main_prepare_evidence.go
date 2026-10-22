package main

import (
	"context"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func dedicatedTimestampCutoverOptions(groups map[uint64]*kv.ShardGroup) []adapter.InternalOption {
	if group := groups[dedicatedTSORaftGroupID]; group != nil {
		return []adapter.InternalOption{adapter.WithInternalTSOCutoverState(group.TSOState)}
	}
	return nil
}

type prepareEvidenceMembershipGate struct {
	probe   func(context.Context, uint64, string) error
	groupID uint64
	next    raftadmin.MembershipChangeInterceptor
}

func newPrepareEvidenceMembershipGate(shards *kv.ShardStore, groupID uint64, next raftadmin.MembershipChangeInterceptor) raftadmin.MembershipChangeInterceptor {
	return &prepareEvidenceMembershipGate{probe: shards.CheckPrepareEvidenceMember, groupID: groupID, next: next}
}

func (g *prepareEvidenceMembershipGate) PreAddMember(ctx context.Context, id, address string) error {
	if err := g.probe(ctx, g.groupID, address); err != nil {
		return err
	}
	if g.next != nil {
		return errors.WithStack(g.next.PreAddMember(ctx, id, address))
	}
	return nil
}
