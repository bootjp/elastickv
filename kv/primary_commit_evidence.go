package kv

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// PrimaryCommitEvidence reads only a locally authoritative primary record.
// It never proxies to another group or interprets an unavailable route as a
// missing record, so legacy cross-group resolution remains admissible.
func (s *ShardStore) PrimaryCommitEvidence(ctx context.Context, primaryKey []byte, startTS uint64) (PrimaryCommitEvidence, error) {
	if len(primaryKey) == 0 || startTS == 0 {
		return PrimaryCommitEvidence{}, errors.WithStack(ErrInvalidRequest)
	}
	route, group, version, ok := s.primaryEvidenceRoute(primaryKey)
	if !ok {
		return PrimaryCommitEvidence{}, nil
	}
	readable, err := primaryEvidenceLeaderReady(ctx, group)
	if err != nil || !readable {
		return PrimaryCommitEvidence{}, err
	}
	evidence, err := s.readPrimaryCommitEvidence(ctx, group, route, primaryKey, startTS)
	if err != nil {
		return PrimaryCommitEvidence{}, errors.WithStack(err)
	}
	if !s.primaryEvidenceOwnershipUnchanged(primaryKey, group, route.GroupID, version) {
		return PrimaryCommitEvidence{}, nil
	}
	return evidence, nil
}

func (s *ShardStore) primaryEvidenceOwnershipUnchanged(primaryKey []byte, group *ShardGroup, groupID, version uint64) bool {
	currentRoute, currentGroup, currentVersion, currentOK := s.primaryEvidenceRoute(primaryKey)
	if !currentOK || currentGroup != group || currentVersion != version || currentRoute.GroupID != groupID {
		return false
	}
	engine := engineForGroup(group)
	return engine == nil || isLeaderEngine(engine)
}

func (s *ShardStore) readPrimaryCommitEvidence(ctx context.Context, group *ShardGroup, route distribution.Route, key []byte, startTS uint64) (PrimaryCommitEvidence, error) {
	value, err := s.localGetAt(ctx, group, route, txnCommitKey(key, startTS), ^uint64(0))
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return PrimaryCommitEvidence{}, errors.WithStack(err)
	}
	if errors.Is(err, store.ErrKeyNotFound) {
		prepared, prepareErr := s.primaryPreparedForEvidence(ctx, group, route, key, startTS)
		return PrimaryCommitEvidence{Readable: true, PrimaryPrepared: prepared}, prepareErr
	}
	commitTS, err := decodeTxnCommitRecord(value)
	if err != nil {
		return PrimaryCommitEvidence{}, errors.Wrap(err, "read primary commit evidence")
	}
	return PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: commitTS}, nil
}

func (s *ShardStore) primaryPreparedForEvidence(ctx context.Context, group *ShardGroup, route distribution.Route, key []byte, startTS uint64) (bool, error) {
	value, err := s.localGetAt(ctx, group, route, txnLockKey(key), ^uint64(0))
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, errors.WithStack(err)
	}
	lock, err := decodeTxnLock(value)
	if err != nil {
		return false, errors.Wrap(err, "read prepared primary lock")
	}
	if !lock.IsPrimaryKey || lock.StartTS != startTS || !bytes.Equal(lock.PrimaryKey, key) {
		return false, nil
	}
	value, err = s.localGetAt(ctx, group, route, txnIntentKey(key), ^uint64(0))
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, errors.WithStack(err)
	}
	intent, err := decodeTxnIntent(value)
	if err != nil {
		return false, errors.Wrap(err, "read prepared primary intent")
	}
	return intent.StartTS == startTS, nil
}

func (s *ShardStore) primaryEvidenceRoute(key []byte) (distribution.Route, *ShardGroup, uint64, bool) {
	route, group, version, ok := s.routeAndGroupForKeyWithVersion(key)
	if s != nil && s.partitionResolver != nil {
		if groupID, resolved := s.partitionResolver.ResolveGroup(key); resolved {
			// Preserve catalog migration fences even when the partition map
			// selects a different physical group from the byte-range owner.
			route, version, ok = s.partitionPrimaryEvidenceRoute(key)
			if !ok {
				return distribution.Route{}, nil, version, false
			}
			route.GroupID = groupID
			group, ok = s.groups[groupID]
		} else if s.partitionResolver.RecognisesPartitionedKey(key) {
			return distribution.Route{}, nil, version, false
		}
	}
	readable := ok && group != nil && group.Store != nil && route.State == distribution.RouteStateActive && !routeHasStagedVisibility(route)
	return route, group, version, readable
}

func (s *ShardStore) partitionPrimaryEvidenceRoute(key []byte) (distribution.Route, uint64, bool) {
	if s.engine == nil {
		return distribution.Route{}, 0, false
	}
	return s.engine.GetRouteWithVersion(routeKey(key))
}

func primaryEvidenceLeaderReady(ctx context.Context, group *ShardGroup) (bool, error) {
	engine := engineForGroup(group)
	if engine == nil {
		return true, nil
	}
	if !isLeaderEngine(engine) {
		return false, nil
	}
	readCtx, cancel := context.WithTimeout(ctx, verifyLeaderTimeout)
	defer cancel()
	if _, err := linearizableReadEngineCtx(readCtx, engine); err != nil {
		if ctx.Err() != nil {
			return false, errors.WithStack(ctx.Err())
		}
		return false, nil
	}
	return true, nil
}

var _ PrimaryCommitEvidenceReader = (*ShardStore)(nil)
