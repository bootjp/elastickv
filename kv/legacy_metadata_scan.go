package kv

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const legacyMetadataScanMaxRows = 512

// LegacyMetadataRowReason describes a legacy-location base metadata row.
type LegacyMetadataRowReason string

const (
	// LegacyMetadataServed is a fallback row whose locally led owner is empty.
	LegacyMetadataServed LegacyMetadataRowReason = "served"
	// LegacyMetadataOwnerHasVersion includes an owner's visible tombstone.
	LegacyMetadataOwnerHasVersion LegacyMetadataRowReason = "owner_has_version"
	// LegacyMetadataOwnerLedElsewhere requires no remote presence probe.
	LegacyMetadataOwnerLedElsewhere LegacyMetadataRowReason = "owner_led_elsewhere"
	// LegacyMetadataStaged defers a row while either route has staged visibility.
	LegacyMetadataStaged LegacyMetadataRowReason = "staged_visibility"
)

// LegacyMetadataRow reports a physical row, its logical owner and disposition.
// Delta rows and canonical owner-local metadata are never legacy backlog.
type LegacyMetadataRow struct {
	Row          *store.KVPair
	OwnerGroupID uint64
	Reason       LegacyMetadataRowReason
}

// GroupOwnedScanPage contains served rows and legacy diagnostics from one
// physical page. Resume is the last physical key when the page was full, even
// when no row was served. CatalogVersion identifies this page, not a completed
// pass or a gate-eligible snapshot.
type GroupOwnedScanPage struct {
	Rows           []*store.KVPair
	LegacyRows     []LegacyMetadataRow
	Resume         []byte
	CatalogVersion uint64
	LeaderTerm     uint64
}

// GroupOwnedScanIdentity fences a completed diagnostic pass to the current
// local leader term and catalog version.
func (s *ShardStore) GroupOwnedScanIdentity(ctx context.Context, groupID uint64) (uint64, uint64, error) {
	g, _ := s.groupForID(groupID)
	if g == nil || g.Store == nil {
		return 0, 0, errors.WithStack(ErrLeaderNotFound)
	}
	version := s.ReadRouteVersion()
	var term uint64
	engine := engineForGroup(g)
	if engine != nil {
		status := engine.Status()
		term = status.Term
		if status.State != raftengine.StateLeader || term == 0 {
			return 0, 0, errors.WithStack(raftengine.ErrNotLeader)
		}
	}
	if err := requireLocalMetadataScanLeader(ctx, g); err != nil {
		return 0, 0, err
	}
	if s.ReadRouteVersion() != version || metadataScanTermChanged(engineForGroup(g), term) {
		return 0, 0, errors.WithStack(ErrReadRouteVersionUnavailable)
	}
	return version, term, nil
}

func metadataScanTermChanged(engine raftengine.Engine, term uint64) bool {
	if engine == nil {
		return term != 0
	}
	status := engine.Status()
	return status.State != raftengine.StateLeader || status.Term != term
}

// ScanGroupOwnedPageAt is the local-leader diagnostic form of ScanGroupOwnedAt.
// It reads at most 512 physical rows, makes no remote owner probes, and checks
// that the catalog did not change while the page was classified. The caller
// must preserve read admission and track leadership/catalog identity across
// pages before publishing a completed pass. This method alone is not an audit
// or evidence that migration has completed.
func (s *ShardStore) ScanGroupOwnedPageAt(ctx context.Context, groupID uint64, start, end []byte, limit int, ts uint64) (GroupOwnedScanPage, error) {
	g, version, term, err := s.groupOwnedPageSource(ctx, groupID, limit)
	if err != nil {
		return GroupOwnedScanPage{}, err
	}
	page, err := s.scanGroupOwnedLocalPage(ctx, groupID, g, start, end, limit, ts)
	if err != nil {
		return GroupOwnedScanPage{}, err
	}
	out, err := s.classifyGroupOwnedScanPageAt(ctx, groupID, page, ts)
	if err != nil {
		return GroupOwnedScanPage{}, err
	}
	currentVersion, currentTerm, err := s.GroupOwnedScanIdentity(ctx, groupID)
	if err != nil {
		return GroupOwnedScanPage{}, err
	}
	if currentVersion != version || currentTerm != term {
		return GroupOwnedScanPage{}, errors.WithStack(ErrReadRouteVersionUnavailable)
	}
	out.CatalogVersion = version
	out.LeaderTerm = term
	return out, nil
}

func (s *ShardStore) groupOwnedPageSource(ctx context.Context, groupID uint64, limit int) (*ShardGroup, uint64, uint64, error) {
	if limit <= 0 || limit > legacyMetadataScanMaxRows {
		return nil, 0, 0, errors.New("legacy metadata scan limit must be between 1 and 512")
	}
	g, _ := s.groupForID(groupID)
	if g == nil || g.Store == nil {
		return nil, 0, 0, errors.WithStack(ErrLeaderNotFound)
	}
	version, term, err := s.GroupOwnedScanIdentity(ctx, groupID)
	return g, version, term, err
}

func (s *ShardStore) scanGroupOwnedLocalPage(ctx context.Context, groupID uint64, g *ShardGroup, start, end []byte, limit int, ts uint64) (scanRoutePage, error) {
	if err := s.ensureReadWatermark(ctx, g, ts); err != nil {
		return scanRoutePage{}, err
	}
	route := distribution.Route{GroupID: groupID}
	if engineForGroup(g) == nil {
		return s.scanRouteAtForwardLocalPage(ctx, g, route, start, end, limit, ts)
	}
	return s.scanRouteAtForwardLeaderPage(ctx, g, route, start, end, limit, ts)
}

func (s *ShardStore) classifyGroupOwnedScanPageAt(ctx context.Context, groupID uint64, page scanRoutePage, ts uint64) (GroupOwnedScanPage, error) {
	out := GroupOwnedScanPage{Rows: make([]*store.KVPair, 0, len(page.kvs))}
	if page.full {
		out.Resume = page.advanceKey
	}
	for _, row := range page.kvs {
		if row == nil {
			continue
		}
		legacy, serves, err := s.classifyLegacyMetadataRowAt(ctx, groupID, row, ts)
		if err != nil {
			return GroupOwnedScanPage{}, err
		}
		if serves {
			out.Rows = append(out.Rows, row)
		}
		if legacy != nil {
			out.LegacyRows = append(out.LegacyRows, *legacy)
		}
	}
	return out, nil
}

func requireLocalMetadataScanLeader(ctx context.Context, g *ShardGroup) error {
	if engine := engineForGroup(g); engine != nil && !isLinearizableRaftLeader(ctx, engine) {
		return errors.WithStack(raftengine.ErrNotLeader)
	}
	return nil
}

func (s *ShardStore) classifyLegacyMetadataRowAt(ctx context.Context, groupID uint64, row *store.KVPair, ts uint64) (*LegacyMetadataRow, bool, error) {
	if !isLegacyMigrationMetadataKey(row.Key) {
		serves, err := s.groupServesKeyAt(ctx, groupID, row.Key, ts)
		return nil, serves, err
	}
	owner, _, ok := s.routeAndGroupForKey(row.Key)
	if !ok {
		return nil, false, errors.WithStack(ErrReadRouteVersionUnavailable)
	}
	if owner.GroupID == groupID {
		return nil, !routeHasStagedVisibility(owner), nil
	}
	legacy, ok := s.engine.GetRoute(legacyPointRouteKey(row.Key))
	if !ok || legacy.GroupID != groupID {
		return nil, false, nil
	}
	result := &LegacyMetadataRow{Row: row, OwnerGroupID: owner.GroupID}
	if routeHasStagedVisibility(owner) || routeHasStagedVisibility(legacy) {
		result.Reason = LegacyMetadataStaged
		return result, false, nil
	}
	reason, err := s.legacyMetadataOwnerReasonAt(ctx, owner, row.Key, ts)
	if err != nil {
		return nil, false, err
	}
	result.Reason = reason
	return result, reason == LegacyMetadataServed, nil
}

func (s *ShardStore) legacyMetadataOwnerReasonAt(ctx context.Context, owner distribution.Route, key []byte, ts uint64) (LegacyMetadataRowReason, error) {
	g, ok := s.groupForID(owner.GroupID)
	if !ok || g == nil || g.Store == nil {
		return LegacyMetadataOwnerLedElsewhere, nil
	}
	exists, answered, err := s.routeHasVersionAtOrBefore(ctx, owner, key, ts)
	if err != nil {
		return "", err
	}
	if !answered {
		if engine := engineForGroup(g); engine != nil && !isLinearizableRaftLeader(ctx, engine) {
			return LegacyMetadataOwnerLedElsewhere, nil
		}
		return "", errors.New("legacy metadata owner cannot answer local version presence")
	}
	if !exists {
		return LegacyMetadataServed, nil
	}
	return LegacyMetadataOwnerHasVersion, nil
}

func isLegacyMigrationMetadataKey(key []byte) bool {
	switch {
	case store.IsHashMetaKey(key):
		return !store.IsHashMetaDeltaKey(key) && store.ExtractHashUserKeyFromMeta(key) != nil
	case store.IsSetMetaKey(key):
		return !store.IsSetMetaDeltaKey(key) && store.ExtractSetUserKeyFromMeta(key) != nil
	case store.IsZSetMetaKey(key):
		return !store.IsZSetMetaDeltaKey(key) && store.ExtractZSetUserKeyFromMeta(key) != nil
	case store.IsStreamMetaKey(key):
		return store.ExtractStreamUserKeyFromMeta(key) != nil
	default:
		return false
	}
}
