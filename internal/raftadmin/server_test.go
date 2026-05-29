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
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

type fakeEngine struct {
	mu sync.Mutex

	status  raftengine.Status
	config  raftengine.Configuration
	serving bool

	addVoterCalls       []fakeAddVoterCall
	addLearnerCalls     []fakeAddVoterCall
	promoteLearnerCalls []fakePromoteLearnerCall
	removeServerCalls   []fakeRemoveServerCall
	transferCalls       int
	targetTransferCalls []fakeTransferCall

	// addVoterHook is invoked synchronously inside AddVoter, before
	// recording the call. Tests use this to observe the ordering of
	// the engine call relative to the interceptor's hook so the
	// "interceptor runs before engine.AddVoter" invariant is actually
	// pinned (claude review on PR #872 — without the hook the test
	// just observed that both ran in any order).
	addVoterHook func()
}

type fakeAddVoterCall struct {
	id        string
	address   string
	prevIndex uint64
}

type fakePromoteLearnerCall struct {
	id                  string
	prevIndex           uint64
	minAppliedIndex     uint64
	skipMinAppliedCheck bool
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
	hook := f.addVoterHook
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addVoterCalls = append(f.addVoterCalls, fakeAddVoterCall{id: id, address: address, prevIndex: prevIndex})
	return 11, nil
}

func (f *fakeEngine) AddLearner(_ context.Context, id string, address string, prevIndex uint64) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.addLearnerCalls = append(f.addLearnerCalls, fakeAddVoterCall{id: id, address: address, prevIndex: prevIndex})
	return 33, nil
}

func (f *fakeEngine) PromoteLearner(_ context.Context, id string, prevIndex uint64, minAppliedIndex uint64, skipMinAppliedCheck bool) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.promoteLearnerCalls = append(f.promoteLearnerCalls, fakePromoteLearnerCall{
		id:                  id,
		prevIndex:           prevIndex,
		minAppliedIndex:     minAppliedIndex,
		skipMinAppliedCheck: skipMinAppliedCheck,
	})
	return 44, nil
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

func (f *fakeEngine) RegisterLeaderAcquiredCallback(fn func()) func() {
	// raftadmin doesn't exercise the leader-acquired observer (PR
	// 4-B-3b's SQS leadership-refusal hook lives in main_sqs_*).
	// The stub satisfies the raftengine.Admin interface so the
	// type-assertion in NewServer succeeds; without this method
	// the assertion would silently fall back to admin=nil and every
	// admin RPC would return Unimplemented (regression caught by
	// TestServerMapsEngineAdminMethods after Phase 3.D PR 4-B-3b
	// extended the Admin interface).
	return func() {}
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

	addLearnerResp, err := server.AddLearner(context.Background(), &pb.RaftAdminAddLearnerRequest{
		Id:            "node-4",
		Address:       "127.0.0.1:50054",
		PreviousIndex: 6,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(33), addLearnerResp.Index)

	promoteResp, err := server.PromoteLearner(context.Background(), &pb.RaftAdminPromoteLearnerRequest{
		Id:              "node-4",
		PreviousIndex:   7,
		MinAppliedIndex: 99,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(44), promoteResp.Index)

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
	require.Equal(t, []fakeAddVoterCall{{id: "node-4", address: "127.0.0.1:50054", prevIndex: 6}}, engine.addLearnerCalls)
	require.Equal(t, []fakePromoteLearnerCall{{id: "node-4", prevIndex: 7, minAppliedIndex: 99}}, engine.promoteLearnerCalls)
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

func TestRegisterOperationalServicesRequiresContext(t *testing.T) {
	t.Parallel()

	server := grpc.NewServer()
	var nilCtx context.Context
	require.PanicsWithValue(t, "raftadmin: RegisterOperationalServices requires non-nil context", func() {
		RegisterOperationalServices(nilCtx, server, &fakeEngine{}, []string{"RawKV"})
	})
}

// recordingInterceptor is a test double for MembershipChangeInterceptor
// that records every PreAddMember call and optionally returns a
// pre-configured error so tests can drive the abort branches.
type recordingInterceptor struct {
	mu      sync.Mutex
	calls   []string
	retErr  error
	preHook func(raftID string) // optional callback; useful for ordering assertions
}

func (r *recordingInterceptor) PreAddMember(_ context.Context, raftID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, raftID)
	if r.preHook != nil {
		r.preHook(raftID)
	}
	return r.retErr
}

// TestServer_AddVoter_InvokesInterceptorBeforeConfChange pins Stage 7c
// §5.1 contract item 1: AddVoter calls the interceptor (with the raftID
// from the request) before the engine's AddVoter, in that order. The
// fakeEngine.addVoterHook appends "addVoter" synchronously from inside
// the engine call, so the recorded ordering pins interceptor-before-
// engine, not just that-both-ran (claude review low finding on PR #872).
func TestServer_AddVoter_InvokesInterceptorBeforeConfChange(t *testing.T) {
	t.Parallel()
	var order []string
	engine := &fakeEngine{addVoterHook: func() { order = append(order, "addVoter") }}
	interceptor := &recordingInterceptor{preHook: func(string) { order = append(order, "preAdd") }}
	server := NewServerWithInterceptor(engine, interceptor)
	resp, err := server.AddVoter(context.Background(), &pb.RaftAdminAddVoterRequest{Id: "n42", Address: "127.0.0.1:9999"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []string{"n42"}, interceptor.calls)
	require.Equal(t, []string{"preAdd", "addVoter"}, order)
	require.Equal(t, 1, len(engine.addVoterCalls))
}

// TestServer_AddVoter_InterceptorErrorAbortsConfChange pins §5.1
// contract item 2: a non-nil PreAddMember error aborts AddVoter; the
// engine's AddVoter is never called.
func TestServer_AddVoter_InterceptorErrorAbortsConfChange(t *testing.T) {
	t.Parallel()
	engine := &fakeEngine{}
	sentinel := context.DeadlineExceeded
	interceptor := &recordingInterceptor{retErr: sentinel}
	server := NewServerWithInterceptor(engine, interceptor)
	resp, err := server.AddVoter(context.Background(), &pb.RaftAdminAddVoterRequest{Id: "n42", Address: "127.0.0.1:9999"})
	require.Error(t, err)
	require.Nil(t, resp)
	require.Equal(t, []string{"n42"}, interceptor.calls)
	require.Equal(t, 0, len(engine.addVoterCalls), "engine.AddVoter must NOT be called when PreAddMember errs")
}

// TestServer_AddVoter_NilInterceptorSkipsPreStep pins §5.1 contract
// item 3: with no interceptor installed (the pre-7c posture, or an
// encryption-disabled build), AddVoter proceeds directly to the
// engine's AddVoter as today.
func TestServer_AddVoter_NilInterceptorSkipsPreStep(t *testing.T) {
	t.Parallel()
	engine := &fakeEngine{}
	server := NewServer(engine) // nil interceptor by construction
	resp, err := server.AddVoter(context.Background(), &pb.RaftAdminAddVoterRequest{Id: "n42", Address: "127.0.0.1:9999"})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, 1, len(engine.addVoterCalls))
}

// TestServer_AddLearner_InterceptorContract is the symmetric set for
// AddLearner — combined into one test since the behavior mirrors
// AddVoter exactly.
func TestServer_AddLearner_InterceptorContract(t *testing.T) {
	t.Parallel()
	t.Run("invokes interceptor then conf-change", func(t *testing.T) {
		t.Parallel()
		engine := &fakeEngine{}
		interceptor := &recordingInterceptor{}
		server := NewServerWithInterceptor(engine, interceptor)
		_, err := server.AddLearner(context.Background(), &pb.RaftAdminAddLearnerRequest{Id: "l1", Address: "127.0.0.1:9000"})
		require.NoError(t, err)
		require.Equal(t, []string{"l1"}, interceptor.calls)
		require.Equal(t, 1, len(engine.addLearnerCalls))
	})
	t.Run("interceptor error aborts", func(t *testing.T) {
		t.Parallel()
		engine := &fakeEngine{}
		interceptor := &recordingInterceptor{retErr: context.DeadlineExceeded}
		server := NewServerWithInterceptor(engine, interceptor)
		_, err := server.AddLearner(context.Background(), &pb.RaftAdminAddLearnerRequest{Id: "l1", Address: "127.0.0.1:9000"})
		require.Error(t, err)
		require.Equal(t, 0, len(engine.addLearnerCalls))
	})
	t.Run("nil interceptor skips pre-step", func(t *testing.T) {
		t.Parallel()
		engine := &fakeEngine{}
		server := NewServer(engine)
		_, err := server.AddLearner(context.Background(), &pb.RaftAdminAddLearnerRequest{Id: "l1", Address: "127.0.0.1:9000"})
		require.NoError(t, err)
		require.Equal(t, 1, len(engine.addLearnerCalls))
	})
}
