package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type diagnosticTTLInlineStore struct {
	store.MVCCStore
	pages         []kv.GroupOwnedScanPage
	version       uint64
	term          uint64
	pageCall      int
	oldCalls      int
	identityCalls int
	onPage        func(*diagnosticTTLInlineStore)
}

func (s *diagnosticTTLInlineStore) ScanGroupOwnedAt(context.Context, uint64, []byte, []byte, int, uint64) ([]*store.KVPair, []byte, error) {
	s.oldCalls++
	return nil, nil, nil
}

func (s *diagnosticTTLInlineStore) ScanGroupOwnedPageAt(_ context.Context, _ uint64, _, _ []byte, _ int, _ uint64) (kv.GroupOwnedScanPage, error) {
	page := s.pages[s.pageCall]
	s.pageCall++
	if s.onPage != nil {
		s.onPage(s)
	}
	return page, nil
}

func (s *diagnosticTTLInlineStore) GroupOwnedScanIdentity(context.Context, uint64) (uint64, uint64, error) {
	s.identityCalls++
	return s.version, s.term, nil
}

func legacyTestRow(owner uint64, reason kv.LegacyMetadataRowReason) kv.LegacyMetadataRow {
	return kv.LegacyMetadataRow{Row: &store.KVPair{Key: []byte(store.HashMetaPrefix + "key")}, OwnerGroupID: owner, Reason: reason}
}

func newLegacyPassTestCompactor(st *diagnosticTTLInlineStore) (*DeltaCompactor, ttlInlineMigrationHandler, ttlInlineMigrationScanPlan) {
	c := NewDeltaCompactor(st, newLocalAdapterCoordinator(st))
	h := ttlInlineMigrationHandler{typeName: "ttl-inline-hash", prefix: []byte(store.HashMetaPrefix), extractUserKey: extractHashMetaMigrationUserKey}
	plan := ttlInlineMigrationScanPlan{cursorName: ttlInlineMigrationGroupCursorName(h.typeName, 1), groupID: 1, scanner: st}
	return c, h, plan
}

func TestLegacyMetadataPassPublishesCompleteOwnerCounts(t *testing.T) {
	st := &diagnosticTTLInlineStore{MVCCStore: store.NewMVCCStore(), version: 7, term: 3,
		pages: []kv.GroupOwnedScanPage{
			{CatalogVersion: 7, LeaderTerm: 3, Resume: []byte("cursor"), LegacyRows: []kv.LegacyMetadataRow{
				legacyTestRow(2, kv.LegacyMetadataServed), legacyTestRow(2, kv.LegacyMetadataOwnerHasVersion)}},
			{CatalogVersion: 7, LeaderTerm: 3, LegacyRows: []kv.LegacyMetadataRow{
				legacyTestRow(3, kv.LegacyMetadataOwnerLedElsewhere), legacyTestRow(3, kv.LegacyMetadataStaged)}},
		}}
	c, h, plan := newLegacyPassTestCompactor(st)
	started := time.Now().Add(-time.Second)
	require.NoError(t, c.migrateTTLInlineScanPlan(t.Context(), h, plan, 10, started))
	require.Empty(t, c.LegacyMetadataPassSnapshots(t.Context()), "in-progress page is not published")
	require.NoError(t, c.migrateTTLInlineScanPlan(t.Context(), h, plan, 10, started))
	snapshots := c.LegacyMetadataPassSnapshots(t.Context())
	require.Len(t, snapshots, 1)
	snapshot := snapshots[0]
	require.Equal(t, uint64(1), snapshot.GroupID)
	require.Equal(t, h.typeName, snapshot.Plan)
	require.Equal(t, uint64(7), snapshot.CatalogVersion)
	require.Equal(t, uint64(3), snapshot.LeaderTerm)
	require.Equal(t, started, snapshot.StartedAt)
	require.True(t, snapshot.CompletionEpoch.After(started))
	require.Equal(t, kv.LegacyMetadataServed, st.pages[0].LegacyRows[0].Reason)
	require.Equal(t, LegacyMetadataReasonCounts{Served: 1, OwnerHasVersion: 1}, snapshot.Counts[2])
	require.Equal(t, LegacyMetadataReasonCounts{OwnerLedElsewhere: 1, StagedVisibility: 1}, snapshot.Counts[3])
	require.Zero(t, st.oldCalls, "diagnostic-capable scanners must not use the legacy scan")
	snapshot.Counts[2] = LegacyMetadataReasonCounts{}
	require.Equal(t, uint64(1), c.LegacyMetadataPassSnapshots(t.Context())[0].Counts[2].Served)
}

func TestLegacyMetadataPassDiscardsCatalogAndLeadershipChanges(t *testing.T) {
	for _, change := range []struct {
		name  string
		apply func(*diagnosticTTLInlineStore)
	}{
		{"catalog", func(s *diagnosticTTLInlineStore) { s.version++ }},
		{"leader-term", func(s *diagnosticTTLInlineStore) { s.term++ }},
	} {
		t.Run(change.name, func(t *testing.T) {
			st := &diagnosticTTLInlineStore{MVCCStore: store.NewMVCCStore(), version: 7, term: 3,
				pages: []kv.GroupOwnedScanPage{
					{CatalogVersion: 7, LeaderTerm: 3, Resume: []byte("cursor"), LegacyRows: []kv.LegacyMetadataRow{legacyTestRow(2, kv.LegacyMetadataServed)}},
					{CatalogVersion: 7, LeaderTerm: 3, LegacyRows: []kv.LegacyMetadataRow{legacyTestRow(2, kv.LegacyMetadataOwnerHasVersion)}},
				}}
			c, h, plan := newLegacyPassTestCompactor(st)
			require.NoError(t, c.migrateTTLInlineScanPlan(t.Context(), h, plan, 10, time.Now()))
			change.apply(st)
			require.Error(t, c.migrateTTLInlineScanPlan(t.Context(), h, plan, 10, time.Now()))
			require.Empty(t, c.LegacyMetadataPassSnapshots(t.Context()))
			require.Empty(t, c.cursors[plan.cursorName], "next tick must restart from prefix")
		})
	}
}

func TestLegacyMetadataPassInvalidatesPublishedSnapshot(t *testing.T) {
	st := &diagnosticTTLInlineStore{MVCCStore: store.NewMVCCStore(), version: 7, term: 3,
		pages: []kv.GroupOwnedScanPage{{CatalogVersion: 7, LeaderTerm: 3}}}
	c, h, plan := newLegacyPassTestCompactor(st)
	require.NoError(t, c.migrateTTLInlineScanPlan(t.Context(), h, plan, 10, time.Now()))
	require.Len(t, c.LegacyMetadataPassSnapshots(t.Context()), 1)
	st.term++
	require.Empty(t, c.LegacyMetadataPassSnapshots(t.Context()))
}

func TestLegacyMetadataPassPrunesDepartedGroupPlan(t *testing.T) {
	st := &diagnosticTTLInlineStore{MVCCStore: store.NewMVCCStore(), version: 7, term: 3,
		pages: []kv.GroupOwnedScanPage{{CatalogVersion: 7, LeaderTerm: 3}}}
	c, h, plan := newLegacyPassTestCompactor(st)
	require.NoError(t, c.migrateTTLInlineScanPlan(t.Context(), h, plan, 10, time.Now()))
	require.Len(t, c.LegacyMetadataPassSnapshots(t.Context()), 1)
	c.pruneTTLInlineMigrationPlanStates(nil)
	require.Empty(t, c.LegacyMetadataPassSnapshots(t.Context()))
}

func TestLegacyMetadataPassDiscardsMidPageRouteChange(t *testing.T) {
	st := &diagnosticTTLInlineStore{MVCCStore: store.NewMVCCStore(), version: 7, term: 3,
		pages:  []kv.GroupOwnedScanPage{{CatalogVersion: 7, LeaderTerm: 3}},
		onPage: func(s *diagnosticTTLInlineStore) { s.version++ }}
	c, h, plan := newLegacyPassTestCompactor(st)
	require.Error(t, c.migrateTTLInlineScanPlan(t.Context(), h, plan, 10, time.Now()))
	require.Empty(t, c.LegacyMetadataPassSnapshots(t.Context()))
	require.Empty(t, c.cursors[plan.cursorName])
}

func TestLegacyMetadataPassRestartsWhenNextPageHasNewCatalog(t *testing.T) {
	st := &diagnosticTTLInlineStore{MVCCStore: store.NewMVCCStore(), version: 7, term: 3,
		pages: []kv.GroupOwnedScanPage{
			{CatalogVersion: 7, LeaderTerm: 3, Resume: []byte("cursor"), LegacyRows: []kv.LegacyMetadataRow{legacyTestRow(2, kv.LegacyMetadataServed)}},
			{CatalogVersion: 8, LeaderTerm: 3, LegacyRows: []kv.LegacyMetadataRow{legacyTestRow(2, kv.LegacyMetadataOwnerHasVersion)}},
			{CatalogVersion: 8, LeaderTerm: 3, LegacyRows: []kv.LegacyMetadataRow{legacyTestRow(3, kv.LegacyMetadataStaged)}},
		}}
	c, h, plan := newLegacyPassTestCompactor(st)
	require.NoError(t, c.migrateTTLInlineScanPlan(t.Context(), h, plan, 10, time.Now()))
	st.version = 8
	require.NoError(t, c.migrateTTLInlineScanPlan(t.Context(), h, plan, 10, time.Now()))
	require.Empty(t, c.LegacyMetadataPassSnapshots(t.Context()))
	require.Empty(t, c.cursors[plan.cursorName])
	require.NoError(t, c.migrateTTLInlineScanPlan(t.Context(), h, plan, 10, time.Now()))
	snapshots := c.LegacyMetadataPassSnapshots(t.Context())
	require.Len(t, snapshots, 1)
	require.Equal(t, uint64(8), snapshots[0].CatalogVersion)
	require.Equal(t, map[uint64]LegacyMetadataReasonCounts{3: {StagedVisibility: 1}}, snapshots[0].Counts)
}

func TestLegacyMetadataPagePreservesRequestReadScope(t *testing.T) {
	st := &diagnosticTTLInlineStore{MVCCStore: store.NewMVCCStore(), version: 7, term: 3,
		pages: []kv.GroupOwnedScanPage{{CatalogVersion: 7, LeaderTerm: 3}}}
	request := kv.NewReadRequest(t.Context(), nil, st, nil)
	defer request.Close()
	_, err := requestScanGroupOwnedPageAt(request.Context(), st, 1, nil, nil, 10, 10)
	require.ErrorIs(t, err, kv.ErrReadSnapshotUnissued)
	require.Zero(t, st.pageCall)
	other := kv.NewReadRequest(t.Context(), nil, store.NewMVCCStore(), nil)
	defer other.Close()
	_, err = requestScanGroupOwnedPageAt(other.Context(), st, 1, nil, nil, 10, 10)
	require.ErrorIs(t, err, kv.ErrReadSnapshotScope)
	require.Zero(t, st.pageCall)
}
