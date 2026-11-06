package kv

import (
	"context"

	"github.com/cockroachdb/errors"
)

func (s *ShardStore) partitionOwner(key []byte) (uint64, bool, error) {
	if s == nil || s.partitionResolver == nil {
		return 0, false, nil
	}
	if groupID, ok := s.partitionResolver.ResolveGroup(key); ok {
		if groupID == 0 {
			return 0, true, errors.Wrap(ErrInvalidRequest, "partition transaction owner is invalid")
		}
		return groupID, true, nil
	}
	if s.partitionResolver.RecognisesPartitionedKey(key) {
		return 0, true, errors.Wrap(ErrInvalidRequest, "partition transaction owner is unresolved")
	}
	return 0, false, nil
}

// Resolve the embedded raw key before routeKey can collapse its partition.
// Ordinary point reads retain their existing byte-range routing contract.
func (s *ShardStore) partitionTxnOwner(key []byte) (uint64, bool, error) {
	if s == nil || s.partitionResolver == nil {
		return 0, false, nil
	}
	userKey, wrapped := txnRouteKey(key)
	if !wrapped {
		return 0, false, nil
	}
	return s.partitionOwner(userKey)
}

func (s *ShardStore) partitionTxnGetAtWithReadFence(ctx context.Context, key []byte, ts, explicitGroup, readVersion uint64) ([]byte, bool, error) {
	groupID, partitioned, err := s.partitionTxnOwner(key)
	if !partitioned {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	if explicitGroup != 0 && explicitGroup != groupID {
		return nil, true, errors.Wrap(ErrExplicitGroupRouteOwnerMismatch, "partition transaction owner differs from requested group")
	}
	group, ok := s.groupForID(groupID)
	if !ok || group == nil || group.Store == nil {
		// No local owner is not proof that a primary record is absent.
		return nil, true, errors.Wrap(ErrLeaderNotFound, "partition transaction owner is unavailable")
	}
	readVersion = max(readVersion, s.ReadRouteVersion())
	value, err := s.getGroupAtWithReadFence(ctx, groupID, key, ts, readVersion)
	return value, true, err
}

func (s *ShardStore) validatePartitionTxnGroup(key []byte, explicitGroup uint64) (bool, error) {
	groupID, partitioned, err := s.partitionTxnOwner(key)
	if err != nil {
		return false, err
	}
	if partitioned && groupID != explicitGroup {
		return false, errors.Wrap(ErrExplicitGroupRouteOwnerMismatch, "partition transaction owner differs from requested group")
	}
	return partitioned, nil
}

func (s *ShardStore) groupForTxnPrimary(key []byte) (*ShardGroup, bool, error) {
	groupID, partitioned, err := s.partitionOwner(key)
	if err != nil {
		return nil, false, err
	}
	if !partitioned {
		group, ok := s.groupForKey(key)
		return group, ok, nil
	}
	group, ok := s.groupForID(groupID)
	if !ok || group == nil || group.Store == nil {
		return nil, false, errors.Wrap(ErrLeaderNotFound, "partition transaction owner is unavailable")
	}
	return group, true, nil
}
