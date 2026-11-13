package serverwiring

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

var _ autosplit.Splitter = DistributionSplitter{}

func TestSchedulerConfigDisabledSkipsValidation(t *testing.T) {
	t.Parallel()

	k, a := parseWiringFlags(t, "-autoSplitMaxRoutes=0")
	cfg, err := a.SchedulerConfig(nil, k)
	require.NoError(t, err)
	require.False(t, cfg.Enabled)
}

func TestSchedulerConfigRejectsRouteCapAboveKeyVizCapacity(t *testing.T) {
	t.Parallel()

	k, a := parseWiringFlags(t, "-autoSplit", "-autoSplitMaxRoutes=10", "-keyvizMaxTrackedRoutes=5")
	_, err := a.SchedulerConfig(nil, k)
	require.ErrorContains(t, err, "--keyvizMaxTrackedRoutes")
}

func TestSchedulerConfigCrossGroupRequiresAutoSplit(t *testing.T) {
	t.Parallel()
	k, a := parseWiringFlags(t, "-autoSplitCrossGroup")
	_, err := a.SchedulerConfig(nil, k)
	require.ErrorContains(t, err, "requires --autoSplit")

	k, a = parseWiringFlags(t, "-autoSplit", "-autoSplitCrossGroup")
	cfg, err := a.SchedulerConfig(nil, k)
	require.NoError(t, err)
	require.True(t, cfg.Enabled)
	require.True(t, cfg.CrossGroupEnabled)
}

// An explicit --keyvizKeyBucketsPerRoute means --autoSplitDefaultBuckets is
// unused, so an invalid value there must not block startup.
func TestValidateSamplerAllowsExplicitBucketsWithUnusedInvalidDefault(t *testing.T) {
	t.Parallel()

	k, a := parseWiringFlags(t, "-autoSplit", "-keyvizKeyBucketsPerRoute=8",
		"-autoSplitDefaultBuckets=1")
	require.NoError(t, a.validateSampler(autosplit.DefaultConfig(), k))

	k, a = parseWiringFlags(t, "-autoSplit", "-autoSplitDefaultBuckets=1")
	require.Equal(t, keyviz.DefaultKeyBucketsPerRoute, *k.KeyBucketsPerRoute)
	require.ErrorContains(t, a.validateSampler(autosplit.DefaultConfig(), k), "--autoSplitDefaultBuckets")
}

func TestValidateDetectorConfigRejectsNonFiniteFloats(t *testing.T) {
	t.Parallel()

	valid := autosplit.DefaultConfig()
	tests := []struct {
		name   string
		mutate func(*autosplit.Config)
	}{
		{name: "write weight NaN", mutate: func(cfg *autosplit.Config) { cfg.WriteWeight = math.NaN() }},
		{name: "read weight positive infinity", mutate: func(cfg *autosplit.Config) { cfg.ReadWeight = math.Inf(1) }},
		{name: "threshold negative infinity", mutate: func(cfg *autosplit.Config) { cfg.ThresholdOpsMin = math.Inf(-1) }},
		{name: "top key share NaN", mutate: func(cfg *autosplit.Config) { cfg.TopKeyShare = math.NaN() }},
		{name: "absolute floor positive infinity", mutate: func(cfg *autosplit.Config) { cfg.TopKeyAbsoluteFloor = math.Inf(1) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := valid
			tc.mutate(&cfg)
			require.Error(t, validateDetectorConfig(cfg))
		})
	}
}

func TestLeaderGateUsesCatalogKeyLeadership(t *testing.T) {
	t.Parallel()

	leader := &fakeKeyLeader{catalogLeader: true}
	gate := leaderGate(leader)

	require.True(t, gate())
	require.True(t, bytes.Equal(distribution.CatalogVersionKey(), leader.lastKey))
	require.False(t, leader.defaultLeaderCalled)
}

func TestLeaderGateFallsBackToDefaultLeader(t *testing.T) {
	t.Parallel()

	leader := &fakeLeader{leader: true}
	gate := leaderGate(leader)

	require.True(t, gate())
	require.True(t, leader.called)
}

// SplitRange copies the parent's migration fields onto both children, so the
// splitter must convert them with the catalog watcher's full converter. Both
// server binaries used converters that dropped StagedVisibilityActive,
// MigrationJobID and MinWriteTSExclusive.
func TestSplitResultFromProtoKeepsRouteFields(t *testing.T) {
	t.Parallel()

	child := func(id uint64, start, end string) *pb.RouteDescriptor {
		return &pb.RouteDescriptor{
			RouteId:                id,
			Start:                  []byte(start),
			End:                    []byte(end),
			RaftGroupId:            3,
			State:                  pb.RouteState_ROUTE_STATE_ACTIVE,
			ParentRouteId:          2,
			SplitAtHlc:             100,
			StagedVisibilityActive: true,
			MigrationJobId:         9,
			MinWriteTsExclusive:    1234,
		}
	}
	got, err := splitResultFromProto(&pb.SplitRangeResponse{
		CatalogVersion: 5,
		Left:           child(7, "a", "m"),
		Right:          child(8, "m", "z"),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), got.CatalogVersion)
	want := func(id uint64, start, end string) distribution.RouteDescriptor {
		return distribution.RouteDescriptor{
			RouteID:                id,
			Start:                  []byte(start),
			End:                    []byte(end),
			GroupID:                3,
			State:                  distribution.RouteStateActive,
			ParentRouteID:          2,
			SplitAtHLC:             100,
			StagedVisibilityActive: true,
			MigrationJobID:         9,
			MinWriteTSExclusive:    1234,
		}
	}
	require.Equal(t, want(7, "a", "m"), got.Left)
	require.Equal(t, want(8, "m", "z"), got.Right)
}

func TestSplitResultFromProtoRejectsMissingChild(t *testing.T) {
	t.Parallel()

	_, err := splitResultFromProto(&pb.SplitRangeResponse{CatalogVersion: 5})
	require.ErrorContains(t, err, "decode left child")
}

func TestSplitMigrationResultFromProto(t *testing.T) {
	t.Parallel()
	got, err := splitMigrationResultFromProto(&pb.StartSplitMigrationResponse{CatalogVersion: 7, JobId: 42})
	require.NoError(t, err)
	require.Equal(t, uint64(7), got.CatalogVersion)
	require.Equal(t, uint64(42), got.JobID)
	require.Equal(t, distribution.RouteDescriptor{}, got.Left)
	require.Equal(t, distribution.RouteDescriptor{}, got.Right)
}

func TestSplitMigrationResultFromProtoRejectsMissingJob(t *testing.T) {
	t.Parallel()
	for _, resp := range []*pb.StartSplitMigrationResponse{nil, {CatalogVersion: 7}} {
		_, err := splitMigrationResultFromProto(resp)
		require.ErrorContains(t, err, "no job ID")
	}
}

func TestSplitMigrationRequest(t *testing.T) {
	t.Parallel()
	splitKey := []byte("m")
	got := splitMigrationRequest(autosplit.SplitRequest{
		ExpectedCatalogVersion: 7,
		RouteID:                3,
		SplitKey:               splitKey,
		TargetGroupID:          2,
	})
	require.Equal(t, uint64(7), got.GetExpectedCatalogVersion())
	require.Equal(t, uint64(3), got.GetRouteId())
	require.Equal(t, []byte("m"), got.GetSplitKey())
	require.Equal(t, uint64(2), got.GetTargetGroupId())
	splitKey[0] = 'x'
	require.Equal(t, []byte("m"), got.GetSplitKey())
}

func TestDistributionSplitterCrossGroupUsesMigrationPreflight(t *testing.T) {
	t.Parallel()
	server := adapter.NewDistributionServer(nil, nil, adapter.WithDistributionCoordinator(readyCatalogLeader{}))
	_, err := (DistributionSplitter{Server: server}).SplitRange(context.Background(), autosplit.SplitRequest{
		ExpectedCatalogVersion: 7,
		RouteID:                1,
		SplitKey:               []byte("m"),
		TargetGroupID:          2,
	})
	require.ErrorContains(t, err, "autosplit: start split migration")
	require.ErrorContains(t, err, "route catalog is not configured")
}

type readyCatalogLeader struct{ kv.Coordinator }

func (readyCatalogLeader) IsLeaderForKey([]byte) bool { return true }

func (readyCatalogLeader) VerifyLeaderForKey(context.Context, []byte) error { return nil }

type fakeLeader struct {
	leader bool
	called bool
}

func (f *fakeLeader) IsLeader() bool {
	f.called = true
	return f.leader
}

type fakeKeyLeader struct {
	catalogLeader       bool
	defaultLeaderCalled bool
	lastKey             []byte
}

func (f *fakeKeyLeader) IsLeader() bool {
	f.defaultLeaderCalled = true
	return false
}

func (f *fakeKeyLeader) IsLeaderForKey(key []byte) bool {
	f.lastKey = append(f.lastKey[:0], key...)
	return f.catalogLeader
}
