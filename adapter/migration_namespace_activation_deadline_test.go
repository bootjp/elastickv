package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type namespaceDeadlineAttempt struct {
	ctx     context.Context
	request *pb.CleanupMigrationRequest
}

type namespaceDeadlineClient struct {
	splitMigrationClientStub
	started chan namespaceDeadlineAttempt
	stop    chan struct{}
}

func (*namespaceDeadlineClient) ProbeMigrationState(context.Context, *pb.ProbeMigrationStateRequest, ...grpc.CallOption) (*pb.ProbeMigrationStateResponse, error) {
	return &pb.ProbeMigrationStateResponse{NamespaceAdmissionVersion: kv.MigrationNamespaceVersion, AppliedConfigurationIndex: 9}, nil
}

func (c *namespaceDeadlineClient) CleanupMigration(ctx context.Context, req *pb.CleanupMigrationRequest, _ ...grpc.CallOption) (*pb.CleanupMigrationResponse, error) {
	c.started <- namespaceDeadlineAttempt{ctx: ctx, request: req}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.stop:
		return nil, context.Canceled
	}
}

func startNamespaceDeadlineAttempt(t *testing.T, ctx context.Context) (namespaceDeadlineAttempt, <-chan error) {
	t.Helper()
	client := &namespaceDeadlineClient{started: make(chan namespaceDeadlineAttempt, 1), stop: make(chan struct{})}
	result, finished := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(finished)
		result <- EnsureMigrationNamespaceAdmission(ctx, client, []SplitMigrationVoter{{ID: "n1", Address: "n1:7001", Client: client}})
	}()
	// Release and join even on the unfixed path, which has no RPC deadline.
	t.Cleanup(func() { close(client.stop); <-finished })
	select {
	case attempt := <-client.started:
		require.NoError(t, kv.ValidateNamespaceAdmissionRequest(attempt.request))
		require.Equal(t, pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_NAMESPACE_ADMISSION, attempt.request.GetMode())
		return attempt, result
	case err := <-result:
		t.Fatalf("namespace activation returned before the MODE8 RPC: %v", err)
	case <-time.After(3 * defaultSplitMigrationAttemptTimeout):
		t.Fatal("namespace activation did not reach the MODE8 RPC")
	}
	return namespaceDeadlineAttempt{}, nil
}

func namespaceDeadlineResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(3 * defaultSplitMigrationAttemptTimeout):
		t.Fatal("namespace activation did not release the controller")
		return nil
	}
}

func TestNamespaceAdmissionActivationAttemptBoundsHungRPC(t *testing.T) {
	parent := context.Background()
	attempt, result := startNamespaceDeadlineAttempt(t, parent)
	deadline, bounded := attempt.ctx.Deadline()
	require.True(t, bounded, "MODE8 needs its own deadline even when the controller context lives forever")
	require.LessOrEqual(t, time.Until(deadline), defaultSplitMigrationAttemptTimeout)
	require.ErrorIs(t, namespaceDeadlineResult(t, result), context.DeadlineExceeded)
	require.ErrorIs(t, attempt.ctx.Err(), context.DeadlineExceeded)
	require.NoError(t, parent.Err(), "one stalled member must not cancel the controller lifecycle")
}

func TestNamespaceAdmissionActivationAttemptHonorsParentCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	attempt, result := startNamespaceDeadlineAttempt(t, parent)
	cancel()
	require.ErrorIs(t, namespaceDeadlineResult(t, result), context.Canceled)
	require.ErrorIs(t, attempt.ctx.Err(), context.Canceled)
}

func TestNamespaceAdmissionActivationAttemptKeepsEarlierParentDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), namespaceProbeTimeout/10)
	t.Cleanup(cancel)
	attempt, result := startNamespaceDeadlineAttempt(t, parent)
	want, bounded := parent.Deadline()
	require.True(t, bounded)
	got, bounded := attempt.ctx.Deadline()
	require.True(t, bounded)
	require.Equal(t, want, got, "a per-attempt budget must never extend the caller deadline")
	require.ErrorIs(t, namespaceDeadlineResult(t, result), context.DeadlineExceeded)
}
