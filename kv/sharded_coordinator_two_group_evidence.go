package kv

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

const TwoGroupPrepareEvidenceProbeVersion = 1

// TwoGroupPrepareEvidenceProbe is a trusted, private leader route. The remote
// implementation must authenticate a group-scoped peer session and read the
// replicated marker after a term-fenced applied ReadIndex.
type TwoGroupPrepareEvidenceProbe interface {
	RequireTwoGroupPrepareEvidence(context.Context, uint64) error
}

func (c *ShardedCoordinator) WithTwoGroupRemoteEvidence(resolve func(uint64) (TwoGroupPrepareEvidenceProbe, error)) *ShardedCoordinator {
	c.twoGroupRemoteEvidence = resolve
	return c
}

func (c *ShardedCoordinator) twoGroupPrepareEvidenceMetas(ctx context.Context, primaryKey []byte,
	commitTS uint64, ids []uint64) (map[uint64]*pb.Mutation, error) {
	if c == nil || len(ids) != 2 || ids[0] == ids[1] {
		return nil, errors.WithStack(ErrTwoGroupAdmission)
	}
	for _, id := range ids {
		if err := c.requireTwoGroupPrepareEvidence(ctx, id); err != nil {
			return nil, err
		}
	}
	if err := reservePrepareEvidenceTimestamp(ctx, c.tsAllocator, commitTS); err != nil {
		return nil, err
	}
	meta := TxnMeta{PrimaryKey: primaryKey, LockTTLms: defaultTxnLockTTLms,
		CommitTS: commitTS, PrepareCommitTS: commitTS}
	metas := make(map[uint64]*pb.Mutation, len(ids))
	for _, id := range ids {
		metas[id] = &pb.Mutation{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(meta)}
	}
	return metas, nil
}

func (c *ShardedCoordinator) requireTwoGroupPrepareEvidence(ctx context.Context, group uint64) error {
	if group == 0 {
		return errors.WithStack(ErrTwoGroupAdmission)
	}
	if local := c.groups[group]; local != nil && local.Store != nil {
		enabled, err := PrepareEvidenceEnabled(ctx, local.Store)
		if err != nil || !enabled {
			return errors.Join(ErrTwoGroupAdmission, err)
		}
		return nil
	}
	if c.twoGroupRemoteEvidence == nil {
		return errors.WithStack(ErrTwoGroupAdmission)
	}
	remote, err := c.twoGroupRemoteEvidence(group)
	if err != nil || remote == nil {
		return errors.Join(ErrTwoGroupAdmission, err)
	}
	return errors.Wrap(remote.RequireTwoGroupPrepareEvidence(ctx, group), "probe remote two-group PREPARE evidence")
}
