package kv

import (
	"context"

	"github.com/cockroachdb/errors"
)

var _ RaftMembershipCoordinator = (*Coordinate)(nil)

// RaftMembers exposes the actual single group's configuration. A missing
// engine is not evidence of a standalone deployment.
func (c *Coordinate) RaftMembers(ctx context.Context) ([]RaftMember, error) {
	if c == nil {
		return nil, errors.WithStack(ErrLeaderNotFound)
	}
	return raftMembersForGroup(ctx, &ShardGroup{Engine: c.engine})
}

func (c *Coordinate) RaftMembersForKey(ctx context.Context, _ []byte) ([]RaftMember, error) {
	return c.RaftMembers(ctx)
}
