package kv

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// Only keys with a single byte-range owner participate in this rollout.
func versionCASDirectKey(key []byte) bool {
	_, _, auxiliary := s3BucketAuxiliaryRouteRange(key)
	return IsCASUserKey(key) && !auxiliary && legacyPointRouteKey(key) == nil &&
		bytes.Equal(key, routeKey(key)) && bytes.Equal(key, routeFilterKey(key))
}

func versionCASInitialRoute(route distribution.Route) bool {
	return route.State == distribution.RouteStateActive && route.MinWriteTSExclusive == 0 &&
		!route.StagedVisibilityActive && route.MigrationJobID == 0
}

// ResolveVersionCASGroupForKey applies the version-token routing restriction
// to the receiving leader's catalog before it accepts a forwarded CAS.
func (s *ShardStore) ResolveVersionCASGroupForKey(key []byte) (uint64, error) {
	route, _, ok := s.versionReadRoute(key, 0)
	if !ok {
		return 0, errors.Wrap(ErrVersionCASUnavailable, "route has migration history or is unavailable")
	}
	return route.GroupID, nil
}

// Check the local Raft-owned guard as well as the catalog route. In particular,
// a source with a stale catalog must stop admitting tokens and CAS once it
// applies a migration guard. Completed moves retain durable ownership
// evidence, and their catalog write floor is preserved through later splits.
// Exclude the entire range, including versions above that floor.
func checkVersionCASMigrationRange(ctx context.Context, st store.MVCCStore, key []byte) error {
	if !versionCASDirectKey(key) {
		return errors.Wrap(ErrVersionCASUnavailable, "key requires alternate routing")
	}
	view, err := readMigrationServingState(ctx, st)
	if err != nil {
		return errors.WithStack(err)
	}
	if migrationApplyIntersects(view, []migrationApplyRange{{key, nextScanCursor(key)}}) {
		return errors.Wrap(ErrVersionCASUnavailable, "range has migration ownership or readiness state")
	}
	return nil
}

// ConfigureVersionReadAdmission installs the protected group runtime's
// admission check before the group starts serving RPCs.
func (g *ShardGroup) ConfigureVersionReadAdmission(check func(context.Context) error) {
	g.versionReadAdmission = check
}

func (s *ShardStore) readVersionOnInitialRoute(ctx context.Context, g *ShardGroup, route distribution.Route, key []byte, ts uint64) (store.VersionedValue, error) {
	if !versionCASInitialRoute(route) {
		return store.VersionedValue{}, store.ErrNotSupported
	}
	if err := checkVersionCASMigrationRange(ctx, g.Store, key); err != nil {
		return store.VersionedValue{}, err
	}
	if err := g.versionReadAdmission(ctx); err != nil {
		return store.VersionedValue{}, err
	}
	record, err := store.GetVersionAt(ctx, g.Store, key, ts)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return store.VersionedValue{}, errors.WithStack(err)
	}
	if _, _, ok := s.versionReadRoute(key, route.GroupID); !ok {
		return store.VersionedValue{}, store.ErrNotSupported
	}
	if err := checkVersionCASMigrationRange(ctx, g.Store, key); err != nil {
		return store.VersionedValue{}, err
	}
	return record, errors.WithStack(err)
}
