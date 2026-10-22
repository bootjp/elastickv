package adapter

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const prepareEvidenceBudgetToken = "peer-token"

func unfilteredPrepareEvidenceClient(t *testing.T, server *Internal) pb.InternalClient {
	t.Helper()
	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	pb.RegisterInternalServer(srv, server)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.Stop(); _ = lis.Close() })
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return pb.NewInternalClient(conn)
}

func TestPrepareEvidenceRPCWithoutAdminInterceptorRequiresCredential(t *testing.T) {
	leader := &prepareSafetyRPCLeader{}
	client := unfilteredPrepareEvidenceClient(t,
		NewInternalWithEngine(nil, leader, nil, nil, WithInternalStore(store.NewMVCCStore())))
	for _, capability := range []bool{false, true} {
		for _, token := range []string{"", "unconfigured-token"} {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
			response, err := client.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{CapabilityOnly: capability})
			cancel()
			require.Equal(t, codes.Unauthenticated, status.Code(err))
			require.Nil(t, response)
		}
	}
	require.Zero(t, leader.reads.Load(), "missing server credential must refuse before quorum work")
}

func TestPrepareEvidenceRPCChecksConfiguredCredentialWithoutInterceptor(t *testing.T) {
	leader := &prepareSafetyRPCLeader{}
	client := unfilteredPrepareEvidenceClient(t, NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalStore(store.NewMVCCStore()), WithInternalPrepareEvidenceToken(prepareEvidenceBudgetToken)))
	for _, token := range []string{"", "wrong", "unrelated-blob-token"} {
		ctx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+token)
		response, err := client.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{})
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.Nil(t, response)
	}
	require.Zero(t, leader.reads.Load())
	ctx := metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer "+prepareEvidenceBudgetToken)
	response, err := client.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{})
	require.NoError(t, err)
	require.Equal(t, uint64(23), response.GetAppliedIndex())
	require.Equal(t, int32(1), leader.reads.Load())
}

type prepareEvidenceBudgetLeader struct {
	prepareSafetyRPCLeader
	read func(context.Context) (uint64, error)
}

func (l *prepareEvidenceBudgetLeader) LinearizableRead(ctx context.Context) (uint64, error) {
	l.reads.Add(1)
	return l.read(ctx)
}

func TestPrepareEvidenceRPCBoundsFenceAndPreservesEarlierDeadline(t *testing.T) {
	for _, timeout := range []time.Duration{0, time.Second, time.Hour} {
		t.Run(timeout.String(), func(t *testing.T) {
			observed := make(chan time.Time, 1)
			leader := &prepareEvidenceBudgetLeader{read: func(ctx context.Context) (uint64, error) {
				deadline, _ := ctx.Deadline()
				observed <- deadline
				return 0, errors.New("stop after observing fence budget")
			}}
			client := unfilteredPrepareEvidenceClient(t, NewInternalWithEngine(nil, leader, nil, nil,
				WithInternalStore(store.NewMVCCStore()), WithInternalPrepareEvidenceToken(prepareEvidenceBudgetToken)))
			ctx := t.Context()
			if timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+prepareEvidenceBudgetToken)
			response, err := client.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{})
			require.Error(t, err)
			require.Nil(t, response)
			deadline := <-observed
			require.False(t, deadline.IsZero(), "the server must supply a deadline even when the caller does not")
			require.Positive(t, time.Until(deadline))
			require.LessOrEqual(t, time.Until(deadline), 5*time.Second)
			if timeout == time.Second {
				require.LessOrEqual(t, time.Until(deadline), time.Second)
			}
		})
	}
}

func TestPrepareEvidenceRPCRejectsConcurrentScansBeforeQuorumWork(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	defer close(release)
	leader := &prepareEvidenceBudgetLeader{read: func(ctx context.Context) (uint64, error) {
		started <- struct{}{}
		select {
		case <-release:
			return 23, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}}
	client := unfilteredPrepareEvidenceClient(t, NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalStore(store.NewMVCCStore()), WithInternalPrepareEvidenceToken(prepareEvidenceBudgetToken)))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+prepareEvidenceBudgetToken)
	finished := make(chan error, 1)
	go func() {
		_, err := client.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{})
		finished <- err
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first inspection did not reach its leader fence")
	}
	response, err := client.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Nil(t, response)
	require.Equal(t, int32(1), leader.reads.Load())
	capability, err := client.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{CapabilityOnly: true})
	require.NoError(t, err)
	require.True(t, capability.GetSupported(), "bounded inspection must not stall capability negotiation")
	release <- struct{}{}
	require.NoError(t, <-finished)
	release <- struct{}{}
	_, err = client.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{})
	require.NoError(t, err, "a completed scan must release its concurrency permit")
	require.Equal(t, int32(2), leader.reads.Load())
}

type prepareEvidenceBudgetStore struct {
	store.MVCCStore
	scan func(context.Context, []byte, []byte, int, uint64) ([]*store.KVPair, error)
}

func (s *prepareEvidenceBudgetStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	return s.scan(ctx, start, end, limit, ts)
}

func TestPrepareEvidenceRPCScanSharesFenceDeadlineAndNeverReturnsPartialDrain(t *testing.T) {
	base := store.NewMVCCStore()
	require.Nil(t, kv.NewKvFSMWithHLC(base, kv.NewHLC()).Apply(kv.EncodePrepareEvidenceFence(0)))
	st := &prepareEvidenceBudgetStore{MVCCStore: base}
	observed := make(chan time.Time, 2)
	scans := make(chan struct{}, 2)
	st.scan = func(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
		scans <- struct{}{}
		if len(scans) == 1 {
			return base.ScanAt(ctx, start, end, limit, ts)
		}
		deadline, _ := ctx.Deadline()
		observed <- deadline
		<-ctx.Done()
		return nil, ctx.Err()
	}
	leader := &prepareEvidenceBudgetLeader{read: func(ctx context.Context) (uint64, error) {
		deadline, _ := ctx.Deadline()
		observed <- deadline
		return 23, nil
	}}
	client := unfilteredPrepareEvidenceClient(t, NewInternalWithEngine(nil, leader, nil, nil,
		WithInternalStore(st), WithInternalPrepareEvidenceToken(prepareEvidenceBudgetToken)))
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+prepareEvidenceBudgetToken)
	response, err := client.GetPrepareEvidenceState(ctx, &pb.PrepareEvidenceStateRequest{})
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	require.Nil(t, response, "an incomplete staged-lock inspection cannot attest that locks drained")
	require.Eventually(t, func() bool { return len(observed) == 2 }, time.Second, time.Millisecond)
	require.Equal(t, <-observed, <-observed, "the staged scan must not reset the leader fence's budget")
	require.Len(t, scans, 2)
}
