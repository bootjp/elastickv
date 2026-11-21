package kv

import (
	"bytes"
	"context"
	"math"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
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

// versionCASCatalogRoute checks the catalog in this group's own store. The
// catalog's version and route records must belong to this group. A remote catalog
// observation cannot prove the ordering of changes in this group's log.
func versionCASCatalogRoute(ctx context.Context, st store.MVCCStore, groupID uint64, key []byte, readTS, minVersion uint64) (uint64, error) {
	catalog, err := distribution.NewCatalogStore(st).SnapshotAt(ctx, readTS)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if catalog.Version == 0 || catalog.Version < minVersion {
		return 0, errors.New("version CAS lacks an applied catalog at or above its admitted version")
	}
	routes, err := distribution.RouteSnapshotFromCatalog(catalog)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if !routes.OwnsCatalogRoutes(groupID) {
		return 0, errors.New("version CAS group does not host the applied catalog")
	}
	route, ok := routes.RouteOf(key)
	if !ok || route.GroupID != groupID || route.State == distribution.RouteStateWriteFenced {
		return 0, errors.Wrap(ErrRouteWriteFenced, "applied catalog does not authorize writing the CAS key in this group")
	}
	if !versionCASInitialRoute(route) {
		return 0, errors.Wrap(ErrVersionCASUnavailable, "applied catalog route has migration history or is inactive")
	}
	return catalog.Version, nil
}

// admitVersionCASRoute is advisory. The indexed FSM checks the same group's
// catalog again before changing data.
func admitVersionCASRoute(ctx context.Context, st store.MVCCStore, groupID uint64, key []byte) (uint64, error) {
	version, err := versionCASCatalogRoute(ctx, st, groupID, key, st.LastCommitTS(), 0)
	if errors.Is(err, ErrRouteWriteFenced) {
		// Admission has not submitted a request. Reserve the route-fence
		// verdict for an indexed rejection.
		return 0, errors.Wrapf(ErrVersionCASUnavailable, "%v", err)
	}
	if err != nil {
		return 0, errors.Join(ErrVersionCASUnavailable, err)
	}
	return version, nil
}

// checkAppliedVersionCASRoute uses the stage's indexed MVCC view. Reading at
// MaxUint64 includes any earlier staged catalog writes and needs no local HLC
// or follower watcher.
func (f *kvFSM) checkAppliedVersionCASRoute(ctx context.Context, groupID, version uint64, key []byte) error {
	if groupID != f.shardGroupID {
		return errors.Wrap(ErrRouteWriteFenced, "CAS frame is bound to a different data group")
	}
	if identity := f.NativeGroupIdentity(); identity.Group != groupID || identity.Cluster == (raftbootstrap.UUID{}) {
		return errors.New("version CAS requires the native protected group identity")
	}
	_, err := versionCASCatalogRoute(ctx, f.store, groupID, key, math.MaxUint64, version)
	return err
}

// ResolveVersionCASGroupForKey finds a candidate in the watched catalog.
// Proposal admission independently verifies the receiving group's catalog.
func (s *ShardStore) ResolveVersionCASGroupForKey(key []byte) (uint64, error) {
	route, _, ok := s.versionReadRoute(key, 0)
	if !ok {
		return 0, errors.Wrap(ErrVersionCASUnavailable, "route has migration history or is unavailable")
	}
	return route.GroupID, nil
}

// CheckVersionCASForwardRoute verifies the fixed receiver independently of
// its watcher. Partition-routed keys are outside standalone CAS.
func (s *ShardStore) CheckVersionCASForwardRoute(ctx context.Context, groupID uint64, key []byte) error {
	if _, partitioned, err := s.partitionOwner(key); partitioned || err != nil || !versionCASDirectKey(key) {
		return errors.Wrap(ErrVersionCASUnavailable, "key requires alternate routing")
	}
	group, ok := s.groupForID(groupID)
	if !ok || group == nil || group.Store == nil {
		return errors.Wrap(ErrVersionCASUnavailable, "CAS receiver group is unavailable")
	}
	_, err := admitVersionCASRoute(ctx, group.Store, groupID, key)
	return err
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

func readVersionOnInitialRoute(ctx context.Context, g *ShardGroup, route distribution.Route, key []byte, ts uint64) (store.VersionedValue, error) {
	if !versionCASInitialRoute(route) {
		return store.VersionedValue{}, store.ErrNotSupported
	}
	version, err := admitVersionCASRoute(ctx, g.Store, route.GroupID, key)
	if err != nil {
		return store.VersionedValue{}, err
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
	if err := recheckVersionReadRoute(ctx, g.Store, route.GroupID, key, version); err != nil {
		return store.VersionedValue{}, err
	}
	if err := checkVersionCASMigrationRange(ctx, g.Store, key); err != nil {
		return store.VersionedValue{}, err
	}
	return record, errors.WithStack(err)
}

func recheckVersionReadRoute(ctx context.Context, st store.MVCCStore, groupID uint64, key []byte, version uint64) error {
	current, err := distribution.NewCatalogStore(st).Version(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	if current != version {
		_, err = admitVersionCASRoute(ctx, st, groupID, key)
	}
	return err
}
