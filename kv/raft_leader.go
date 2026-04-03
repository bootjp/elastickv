package kv

import (
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
)

func verifyRaftLeader(r *raft.Raft) error {
	if r == nil {
		return errors.WithStack(ErrLeaderNotFound)
	}
	if err := r.VerifyLeader().Error(); err != nil {
		return errors.WithStack(err)
	}
	return nil
}
