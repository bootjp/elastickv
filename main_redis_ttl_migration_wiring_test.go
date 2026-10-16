package main

import (
	"context"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// groupScanRecordingStore stands in for the *kv.ShardStore the Redis delta
// compactor holds in production. Exposing ScanGroupAt is what lets the
// TTL-inline migrator plan one scan per locally led group instead of a
// full-keyspace scan.
type groupScanRecordingStore struct {
	store.MVCCStore

	mu         sync.Mutex
	groupScans map[uint64][]string
	fullScans  []string
}

func (s *groupScanRecordingStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.mu.Lock()
	s.fullScans = append(s.fullScans, string(start))
	s.mu.Unlock()
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func (s *groupScanRecordingStore) ScanGroupAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.mu.Lock()
	if s.groupScans == nil {
		s.groupScans = make(map[uint64][]string)
	}
	s.groupScans[groupID] = append(s.groupScans[groupID], string(start))
	s.mu.Unlock()
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func (s *groupScanRecordingStore) recorded() (map[uint64][]string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	groupScans := make(map[uint64][]string, len(s.groupScans))
	for groupID, starts := range s.groupScans {
		groupScans[groupID] = append([]string(nil), starts...)
	}
	return groupScans, append([]string(nil), s.fullScans...)
}

// main.go hands the Redis delta compactor a startupGatedCoordinator, and
// NewDeltaCompactor wraps that in WithKeyVizLabel. The TTL-inline migrator asks
// that outermost value which data groups this node leads and scans only those.
// The label wrapper always answers, so when the startup gate in between did not
// forward LocalLeaderGroupIDs the migrator got an empty list, planned no scans,
// and migrated nothing in production. Adapter tests hand the compactor an
// unwrapped coordinator and could not see it, so the real chain is pinned here.
func TestRedisTTLInlineMigrationScansLocalLeaderGroupsThroughStartupGate(t *testing.T) {
	t.Parallel()

	leader := &mainTSOEngine{state: raftengine.StateLeader}
	follower := &mainTSOEngine{state: raftengine.StateFollower}
	coord := kv.NewShardedCoordinator(distribution.NewEngine(), map[uint64]*kv.ShardGroup{
		1: {Engine: leader},
		2: {Engine: leader},
		3: {Engine: follower},
	}, 1, kv.NewHLC(), nil)
	localLeaders := []uint64{1, 2}
	require.Equal(t, localLeaders, coord.LocalLeaderGroupIDs())

	gate := &startupPublicKVGate{}
	gate.markReady()
	gated := startupGatedCoordinator{inner: coord, gate: gate}

	labeled, ok := kv.WithKeyVizLabel(gated, keyviz.LabelRedis).(interface{ LocalLeaderGroupIDs() []uint64 })
	require.True(t, ok, "the Redis compactor's coordinator must expose LocalLeaderGroupIDs")
	require.Equal(t, localLeaders, labeled.LocalLeaderGroupIDs(),
		"the local leader groups must survive the startup gate and the keyviz label")

	st := &groupScanRecordingStore{MVCCStore: store.NewMVCCStore()}
	compactor := adapter.NewDeltaCompactor(st, gated)
	require.NoError(t, compactor.SyncOnce(context.Background()))

	groupScans, fullScans := st.recorded()
	scannedGroups := make([]uint64, 0, len(groupScans))
	for groupID := range groupScans {
		scannedGroups = append(scannedGroups, groupID)
	}
	require.ElementsMatch(t, localLeaders, scannedGroups,
		"the migrator must scan every group this node leads and no group it follows")
	require.Contains(t, groupScans[1], store.HashMetaPrefix)
	require.Equal(t, groupScans[1], groupScans[2],
		"every TTL-inline handler must plan one scan per locally led group")
	seen := make(map[string]struct{}, len(groupScans[1]))
	for _, start := range groupScans[1] {
		_, dup := seen[start]
		require.Falsef(t, dup, "handler prefix %q was scanned twice in one group", start)
		seen[start] = struct{}{}
	}
	require.NotContains(t, fullScans, store.HashMetaPrefix,
		"the migrator must not fall back to a full-keyspace scan when the groups are known")
}
