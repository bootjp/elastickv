package filesystem

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	cerrors "github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// forwardedRootRaceCoordinator mimics a follower whose root-initialization
// proposal is forwarded to the leader and loses the race: the winner's write
// is applied, and the follower observes only the gRPC status envelope that
// LeaderProxy wraps after exhausting its retries.
type forwardedRootRaceCoordinator struct {
	inner     *testCoordinator
	once      sync.Once
	winnerErr error
	wireErr   error
	calls     int
}

func (c *forwardedRootRaceCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c *forwardedRootRaceCoordinator) Dispatch(
	ctx context.Context,
	req *kv.OperationGroup[kv.OP],
) (*kv.CoordinateResponse, error) {
	c.calls++
	var raced bool
	c.once.Do(func() {
		raced = true
		_, c.winnerErr = c.inner.Dispatch(ctx, req)
	})
	if c.winnerErr != nil {
		return nil, c.winnerErr
	}
	if raced {
		return nil, c.wireErr
	}
	return c.inner.Dispatch(ctx, req)
}

func forwardedWriteConflict(key []byte) error {
	wire := status.Error(codes.Unknown, store.NewWriteConflictError(key).Error())
	return cerrors.Wrapf(wire, "leader forward failed after %d retries", 3)
}

func TestServiceInitializeRootRetriesForwardedWriteConflict(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := &forwardedRootRaceCoordinator{
		inner:   &testCoordinator{st: st},
		wireErr: forwardedWriteConflict([]byte("!fs|ino|\x00\x00\x00\x00\x00\x00\x00\x01")),
	}
	svc, err := NewService(st, coord, WithChunkSize(testChunkSize))
	require.NoError(t, err)

	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 0, 0))
	require.Equal(t, 1, coord.calls, "second pass must observe the winner's root instead of proposing again")

	stat, err := svc.GetAttr(ctx, RootInode)
	require.NoError(t, err)
	require.Equal(t, TypeDirectory, stat.Type)
	require.Equal(t, testRootMode, stat.Mode)
}

func TestServiceInitializeRootDoesNotRetryUnrelatedWireErrors(t *testing.T) {
	ctx := context.Background()
	st := store.NewMVCCStore()
	wire := status.Error(codes.Unknown, "key: !fs|ino|: disk full")
	coord := &forwardedRootRaceCoordinator{
		inner:   &testCoordinator{st: store.NewMVCCStore()},
		wireErr: cerrors.Wrap(wire, "leader forward failed after 3 retries"),
	}
	svc, err := NewService(st, coord, WithChunkSize(testChunkSize))
	require.NoError(t, err)

	err = svc.InitializeRoot(ctx, testRootMode, 0, 0)
	require.ErrorContains(t, err, "disk full")
	require.Equal(t, 1, coord.calls)
}

func TestIsWriteConflict(t *testing.T) {
	conflictKey := []byte("!fs|ino|root")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "typed conflict", err: store.NewWriteConflictError(conflictKey), want: true},
		{name: "wrapped typed conflict", err: cerrors.Wrap(store.ErrWriteConflict, "txn"), want: true},
		{name: "unknown attempt with conflict detail", err: errors.Join(kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict), want: false},
		{name: "forwarded unknown status", err: forwardedWriteConflict(conflictKey), want: true},
		{name: "forwarded aborted status", err: status.Error(codes.Aborted, store.NewWriteConflictError(conflictKey).Error()), want: true},
		{name: "forwarded other code", err: status.Error(codes.Unavailable, store.NewWriteConflictError(conflictKey).Error()), want: false},
		{name: "status without key envelope", err: status.Error(codes.Unknown, "write conflict"), want: false},
		{name: "status with other suffix", err: status.Error(codes.Unknown, "key: a: not leader"), want: false},
		{name: "plain error", err: errors.New("key: a: write conflict"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsWriteConflict(tt.err))
			require.Equal(t, tt.want, isRootInitializationConflict(tt.err))
		})
	}
}

func TestRootInitializationRechecksAfterUncertainForwardedConflict(t *testing.T) {
	key := []byte("!fs|ino|root")
	err := errors.Join(kv.ErrTransactionOutcomeUnknown, forwardedWriteConflict(key))
	require.False(t, IsWriteConflict(err))
	require.True(t, isRootInitializationConflict(err))
}
