package raftadmin

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

type fakeEngine struct {
	mu sync.Mutex

	status  raftengine.Status
	config  raftengine.Configuration
	serving bool

	addVoterCalls       []fakeAddVoterCall
	removeServerCalls   []fakeRemoveServerCall
	transferCalls       int
	targetTransferCalls []fakeTransferCall
}

type fakeAddVoterCall struct {
	id        string
	address   string
	prevIndex uint64
}

type fakeRemoveServerCall struct {
	id        string
	prevIndex uint64
}

type fakeTransferCall struct {
	id      string
	address string
}

func (f *fakeEngine) Close() error { return nil }

func (f *fakeEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return &raftengine.ProposalResult{}, nil
}

func (f *fakeEngine) State() raftengine.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status.State
}

func (f *fakeEngine) Leader() raftengine.LeaderInfo {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status.Leader
}

func (f *fakeEngine) VerifyLeader(context.Context) error { return nil }

func (f *fakeEngine) LinearizableRead(context.Context) (uint64, error) { return 0, nil }

func (f *fakeEngine) Status() raftengine.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status
}

func (f *fakeEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.config, nil
}

func (f *fakeEngine) CheckServing(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.serving {
		return nil
	}
	return context.DeadlineExceeded
}

func (f *fakeEngine) AddVoter(_ context.Context, id string, address string, prevIndex uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addVoterCalls = append(f.addVoterCalls, fakeAddVoterCall{id: id, address: address, prevIndex: prevIndex})
	return 11, nil
}

func (f *fakeEngine) RemoveServer(_ context.Context, id string, prevIndex uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removeServerCalls = append(f.removeServerCalls, fakeRemoveServerCall{id: id, prevIndex: prevIndex})
	return 22, nil
}

func (f *fakeEngine) TransferLeadership(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transferCalls++
	return nil
}

func (f *fakeEngine) TransferLeadershipToServer(_ context.Context, id string, address string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.targetTransferCalls = append(f.targetTransferCalls, fakeTransferCall{id: id, address: address})
	return nil
}

func TestServerMapsEngineAdminMethods(t *testing.T) {
	t.Parallel()

	engine := &fakeEngine{
		status: raftengine.Status{
			State: raftengine.StateLeader,
			Leader: raftengine.LeaderInfo{
				ID:      "node-1",
				Address: "127.0.0.1:50051",
			},
			Term:              7,
			CommitIndex:       10,
			AppliedIndex:      9,
			LastLogIndex:      12,
			LastSnapshotIndex: 8,
			FSMPending:        1,
			NumPeers:          2,
			LastContact:       0,
		},
		config: raftengine.Configuration{
			Servers: []raftengine.Server{
				{ID: "node-1", Address: "127.0.0.1:50051", Suffrage: "voter"},
				{ID: "node-2", Address: "127.0.0.1:50052", Suffrage: "voter"},
			},
		},
	}
	server := NewServer(engine)

	statusResp, err := server.Status(context.Background(), &pb.RaftAdminStatusRequest{})
	require.NoError(t, err)
	require.Equal(t, pb.RaftAdminState_RAFT_ADMIN_STATE_LEADER, statusResp.State)
	require.Equal(t, "node-1", statusResp.LeaderId)
	require.Equal(t, uint64(10), statusResp.CommitIndex)

	cfgResp, err := server.Configuration(context.Background(), &pb.RaftAdminConfigurationRequest{})
	require.NoError(t, err)
	require.Len(t, cfgResp.Servers, 2)
	require.Equal(t, "node-2", cfgResp.Servers[1].Id)

	addResp, err := server.AddVoter(context.Background(), &pb.RaftAdminAddVoterRequest{
		Id:            "node-3",
		Address:       "127.0.0.1:50053",
		PreviousIndex: 4,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(11), addResp.Index)

	removeResp, err := server.RemoveServer(context.Background(), &pb.RaftAdminRemoveServerRequest{
		Id:            "node-2",
		PreviousIndex: 5,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(22), removeResp.Index)

	_, err = server.TransferLeadership(context.Background(), &pb.RaftAdminTransferLeadershipRequest{})
	require.NoError(t, err)
	_, err = server.TransferLeadership(context.Background(), &pb.RaftAdminTransferLeadershipRequest{
		TargetId:      "node-2",
		TargetAddress: "127.0.0.1:50052",
	})
	require.NoError(t, err)

	engine.mu.Lock()
	defer engine.mu.Unlock()
	require.Equal(t, []fakeAddVoterCall{{id: "node-3", address: "127.0.0.1:50053", prevIndex: 4}}, engine.addVoterCalls)
	require.Equal(t, []fakeRemoveServerCall{{id: "node-2", prevIndex: 5}}, engine.removeServerCalls)
	require.Equal(t, 1, engine.transferCalls)
	require.Equal(t, []fakeTransferCall{{id: "node-2", address: "127.0.0.1:50052"}}, engine.targetTransferCalls)
}

func TestRegisterOperationalServicesPublishesLeaderHealth(t *testing.T) {
	t.Parallel()

	engine := &fakeEngine{
		status:  raftengine.Status{State: raftengine.StateLeader},
		serving: true,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	RegisterOperationalServices(ctx, server, engine, []string{"RawKV"})
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := healthpb.NewHealthClient(conn)
	require.Eventually(t, func() bool {
		resp, checkErr := client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "RawKV"})
		return checkErr == nil && resp.Status == healthpb.HealthCheckResponse_SERVING
	}, 5*time.Second, 50*time.Millisecond)

	engine.mu.Lock()
	engine.serving = false
	engine.mu.Unlock()

	require.Eventually(t, func() bool {
		resp, checkErr := client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "RawKV"})
		return checkErr == nil && resp.Status == healthpb.HealthCheckResponse_NOT_SERVING
	}, 5*time.Second, 50*time.Millisecond)
}

type stateOnlyEngine struct {
	state raftengine.State
}

func (s stateOnlyEngine) Close() error { return nil }

func (s stateOnlyEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return &raftengine.ProposalResult{}, nil
}

func (s stateOnlyEngine) State() raftengine.State { return s.state }

func (s stateOnlyEngine) Leader() raftengine.LeaderInfo { return raftengine.LeaderInfo{} }

func (s stateOnlyEngine) VerifyLeader(context.Context) error {
	panic("VerifyLeader should not be called from health fallback")
}

func (s stateOnlyEngine) LinearizableRead(context.Context) (uint64, error) { return 0, nil }

func (s stateOnlyEngine) Status() raftengine.Status { return raftengine.Status{State: s.state} }

func (s stateOnlyEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}

func TestCurrentHealthStatusFallsBackToLocalState(t *testing.T) {
	t.Parallel()

	require.Equal(t, healthpb.HealthCheckResponse_SERVING, currentHealthStatus(context.Background(), stateOnlyEngine{state: raftengine.StateLeader}))
	require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, currentHealthStatus(context.Background(), stateOnlyEngine{state: raftengine.StateFollower}))
}

func TestHealthPollIntervalHonorsEnv(t *testing.T) {
	t.Setenv(healthPollIntervalEnv, "900")
	require.Equal(t, 900*time.Millisecond, healthPollInterval())
}

func TestHealthPollIntervalFallsBackToDefault(t *testing.T) {
	t.Setenv(healthPollIntervalEnv, "bad")
	require.Equal(t, defaultHealthPollInterval, healthPollInterval())

	t.Setenv(healthPollIntervalEnv, "0")
	require.Equal(t, defaultHealthPollInterval, healthPollInterval())
}

func TestHealthPollIntervalClampsMinimum(t *testing.T) {
	t.Setenv(healthPollIntervalEnv, "1")
	require.Equal(t, minHealthPollInterval, healthPollInterval())
}

func TestRegisterOperationalServicesWithoutContextPublishesStaticHealth(t *testing.T) {
	t.Parallel()

	engine := &fakeEngine{
		status:  raftengine.Status{State: raftengine.StateLeader},
		serving: true,
	}

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterRaftAdminServer(server, NewServer(engine))
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(server, healthSrv)
	registerStaticHealthService(healthSrv, engine, []string{"RawKV"})
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client := healthpb.NewHealthClient(conn)
	require.Eventually(t, func() bool {
		resp, checkErr := client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "RawKV"})
		return checkErr == nil && resp.Status == healthpb.HealthCheckResponse_SERVING
	}, 5*time.Second, 50*time.Millisecond)

	engine.mu.Lock()
	engine.serving = false
	engine.mu.Unlock()

	resp, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: "RawKV"})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.Status)
}
