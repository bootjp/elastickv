package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDistributionCatalogGroupID_UsesCatalogKeyRoute(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 2)
	engine.UpdateRoute([]byte("m"), nil, 3)

	groupID, err := distributionCatalogGroupID(engine)
	require.NoError(t, err)
	require.Equal(t, uint64(2), groupID)
}

func TestDistributionCatalogGroupID_FailsWhenRouteMissing(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), nil, 7)

	_, err := distributionCatalogGroupID(engine)
	require.Error(t, err)
	require.ErrorContains(t, err, "no shard route for distribution catalog key")
}

func TestDistributionCatalogGroupID_FailsWhenEngineMissing(t *testing.T) {
	t.Parallel()

	_, err := distributionCatalogGroupID(nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "distribution engine is required")
}

func TestSetupDistributionCatalog_FailsWhenRuntimeMissingForResolvedGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 9)

	_, err := setupDistributionCatalog(context.Background(), []*raftGroupRuntime{}, engine)
	require.Error(t, err)
	require.ErrorContains(t, err, "distribution catalog store is not available for group 9")
}

func TestSetupDistributionCatalog_UsesResolvedCatalogGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 5)
	rt := &raftGroupRuntime{
		spec:  groupSpec{id: 5},
		store: store.NewMVCCStore(),
	}
	t.Cleanup(func() {
		rt.Close()
	})

	catalog, err := setupDistributionCatalog(context.Background(), []*raftGroupRuntime{rt}, engine)
	require.NoError(t, err)
	require.NotNil(t, catalog)
}

func TestSplitMigrationCapabilityGateChecksAllPeers(t *testing.T) {
	t.Parallel()

	peers := []splitMigrationCapabilityPeer{
		{ID: "n1", Address: "10.0.0.11:50051"},
		{ID: "n2", Address: "10.0.0.12:50051"},
	}
	var probed []string
	gate := newSplitMigrationCapabilityGate(staticSplitMigrationCapabilityPeerSource(peers), time.Second, func(_ context.Context, address string) error {
		probed = append(probed, address)
		return nil
	})

	require.NoError(t, gate(context.Background()))
	require.Equal(t, []string{"10.0.0.11:50051", "10.0.0.12:50051"}, probed)
}

func TestSplitMigrationCapabilityGateFailsClosedWithoutPeers(t *testing.T) {
	t.Parallel()

	gate := newSplitMigrationCapabilityGate(nil, time.Second, func(context.Context, string) error {
		t.Fatal("probe must not run without peers")
		return nil
	})

	err := gate(context.Background())
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, "peers are not configured")
}

func TestSplitMigrationCapabilityGateFailsClosedWhenPeerMissingCapability(t *testing.T) {
	t.Parallel()

	peers := []splitMigrationCapabilityPeer{
		{ID: "n1", Address: "10.0.0.11:50051"},
		{ID: "n2", Address: "10.0.0.12:50051"},
	}
	gate := newSplitMigrationCapabilityGate(staticSplitMigrationCapabilityPeerSource(peers), time.Second, func(_ context.Context, address string) error {
		if address == "10.0.0.12:50051" {
			return status.Error(codes.Unimplemented, "method not found")
		}
		return nil
	})

	err := gate(context.Background())
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, "n2")
	require.ErrorContains(t, err, "method not found")
}

func TestSplitMigrationCapabilityGateFailsClosedWhenPeerSourceErrors(t *testing.T) {
	t.Parallel()

	errPeerSourceUnavailable := errors.New("configuration unavailable")
	gate := newSplitMigrationCapabilityGate(func(context.Context) ([]splitMigrationCapabilityPeer, error) {
		return nil, errPeerSourceUnavailable
	}, time.Second, func(context.Context, string) error {
		t.Fatal("probe must not run when peer source fails")
		return nil
	})

	err := gate(context.Background())
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.ErrorContains(t, err, "peers are not available")
}

func TestSplitMigrationCapabilityPeersFromConfigurationUsesCurrentMembers(t *testing.T) {
	t.Parallel()

	cfg := raftengine.Configuration{Servers: []raftengine.Server{
		{Suffrage: "voter", ID: "n1", Address: "10.0.0.11:50051"},
		{Suffrage: "learner", ID: "n2", Address: "10.0.0.12:50051"},
		{Suffrage: "", ID: "n3", Address: "10.0.0.13:50051"},
		{Suffrage: "voter", ID: "n1", Address: "10.0.0.11:50051"},
	}}

	require.Equal(t, []splitMigrationCapabilityPeer{
		{ID: "n1", Address: "10.0.0.11:50051"},
		{ID: "n2", Address: "10.0.0.12:50051"},
		{ID: "n3", Address: "10.0.0.13:50051"},
	}, splitMigrationCapabilityPeersFromConfiguration(cfg))
}

func TestSplitMigrationCapabilityPeerSourceForRuntimesChecksAllGroups(t *testing.T) {
	t.Parallel()

	runtimes := []*raftGroupRuntime{
		{
			spec: groupSpec{id: 1},
			engine: capabilityConfigEngine{cfg: raftengine.Configuration{Servers: []raftengine.Server{
				{Suffrage: "voter", ID: "n1", Address: "10.0.0.11:50051"},
				{Suffrage: "learner", ID: "n2", Address: "10.0.0.12:50051"},
			}}},
		},
		{
			spec: groupSpec{id: 2},
			engine: capabilityConfigEngine{cfg: raftengine.Configuration{Servers: []raftengine.Server{
				{Suffrage: "voter", ID: "n1", Address: "10.0.0.11:50051"},
				{Suffrage: "voter", ID: "n3", Address: "10.0.0.13:50051"},
			}}},
		},
	}

	peers, err := splitMigrationCapabilityPeerSourceForRuntimes(runtimes)(context.Background())
	require.NoError(t, err)
	require.Equal(t, []splitMigrationCapabilityPeer{
		{ID: "n1", Address: "10.0.0.11:50051"},
		{ID: "n2", Address: "10.0.0.12:50051"},
		{ID: "n3", Address: "10.0.0.13:50051"},
	}, peers)
}

func staticSplitMigrationCapabilityPeerSource(peers []splitMigrationCapabilityPeer) splitMigrationCapabilityPeerSource {
	return func(context.Context) ([]splitMigrationCapabilityPeer, error) {
		out := make([]splitMigrationCapabilityPeer, len(peers))
		copy(out, peers)
		return out, nil
	}
}

type capabilityConfigEngine struct {
	cfg raftengine.Configuration
}

func (e capabilityConfigEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, nil
}

func (e capabilityConfigEngine) ProposeAdmin(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, nil
}

func (e capabilityConfigEngine) State() raftengine.State {
	return raftengine.StateFollower
}

func (e capabilityConfigEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{}
}

func (e capabilityConfigEngine) VerifyLeader(context.Context) error {
	return nil
}

func (e capabilityConfigEngine) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}

func (e capabilityConfigEngine) Status() raftengine.Status {
	return raftengine.Status{}
}

func (e capabilityConfigEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return e.cfg, nil
}

func (e capabilityConfigEngine) Close() error {
	return nil
}
