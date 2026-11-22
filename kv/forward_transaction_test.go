package kv

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func newForwardPortTest(t *testing.T, options ...TransactionOption) (*ShardGroup, *LeaderProxy, *fakeProposer) {
	t.Helper()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	inner := &fakeProposer{}
	group := &ShardGroup{Engine: &recordingEngineForShardGroup{inner: inner}, Store: st}
	options = append(options, WithMigrationRequestAdmission(st, 7))
	proxy := NewLeaderProxyForShardGroup(group, options...)
	group.Txn = proxy
	t.Cleanup(proxy.tm.Close)
	return group, proxy, inner
}
func TestForwardTransactionPreservesReceiverEpochAndLocalPreparation(t *testing.T) {
	t.Parallel()
	var preparations atomic.Int32
	group, proxy, p := newForwardPortTest(t, WithTimestampAdmission(nil, func() TimestampAllocator { preparations.Add(1); return nil }))
	tx := group.ForwardTransaction(7)
	require.NotNil(t, tx)
	require.True(t, MigrationRequestAdmissionReady(tx))
	require.True(t, MigrationPartitionRequestAdmissionReady(tx))
	request := &pb.Request{Ts: 100, TimestampAdmissionFloor: 77, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("key"), Value: []byte("value")}}}
	_, err := tx.Commit(t.Context(), []*pb.Request{request})
	require.NoError(t, err)
	require.Zero(t, preparations.Load(), "Internal already validated/stamped the receiver epoch")
	var received pb.Request
	require.NoError(t, proto.Unmarshal(p.last[1:], &received))
	require.Equal(t, uint64(77), received.GetTimestampAdmissionFloor())
	_, err = proxy.tm.Commit(t.Context(), []*pb.Request{request})
	require.NoError(t, err)
	require.Equal(t, int32(1), preparations.Load(), "local commits retain their original preparation")
	require.NoError(t, proto.Unmarshal(p.last[1:], &received))
	require.Zero(t, received.GetTimestampAdmissionFloor())
	require.Equal(t, uint64(77), request.TimestampAdmissionFloor, "local preparation owns a clone")
}
func TestForwardTransactionKeepsDynamicWrapAndBorrowedLifetime(t *testing.T) {
	t.Parallel()
	group, proxy, p := newForwardPortTest(t)
	group.Txn = &leaseRefreshingTxn{inner: proxy, g: group}
	tx := group.ForwardTransaction(7)
	require.NotNil(t, tx)
	_, ownsClose := tx.(interface{ Close() error })
	require.False(t, ownsClose)
	for _, marker := range []byte{0xa1, 0xb2} {
		group.SetRaftPayloadWrap(func(data []byte) ([]byte, error) { return append([]byte{marker}, data...), nil })
		_, err := tx.Commit(t.Context(), []*pb.Request{{Ts: 100, Mutations: []*pb.Mutation{{Key: []byte("key")}}}})
		require.NoError(t, err)
		require.Equal(t, marker, p.last[0])
	}
	proxy.tm.Close()
	_, err := tx.Commit(t.Context(), []*pb.Request{{Ts: 101, Mutations: []*pb.Mutation{{Key: []byte("key")}}}})
	require.ErrorIs(t, err, errShuttingDown)
	require.Equal(t, int32(2), p.calls.Load())
}
func TestForwardTransactionRejectsUnknownOrMismatchedOwnership(t *testing.T) {
	t.Parallel()
	group, proxy, _ := newForwardPortTest(t)
	var missing *ShardGroup
	require.Nil(t, missing.ForwardTransaction(7))
	require.Nil(t, group.ForwardTransaction(8))
	group.Txn = proxy.tm
	require.Nil(t, group.ForwardTransaction(7), "custom manager is not a group-owned proxy")
	group.Txn = NewLeaderProxyWithEngine(group.Engine)
	require.Nil(t, group.ForwardTransaction(7), "unbound proxy must retain fallback")
	group.Txn = proxy
	proxy.tm.migrationAdmission.Store(nil)
	require.Nil(t, group.ForwardTransaction(7))
}

type forwardOwnedTestEngine struct {
	raftengine.Engine
	inner *ownedContextProposer
}

func (e *forwardOwnedTestEngine) RequiresOwnedProposalContext() bool { return true }
func (e *forwardOwnedTestEngine) Propose(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return e.inner.Propose(ctx, data)
}
func (e *forwardOwnedTestEngine) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return e.inner.Propose(ctx, data)
}
func TestForwardTransactionRetainsOwnedContextAndUnknown(t *testing.T) {
	t.Parallel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	p := &ownedContextProposer{fakeProposer: &fakeProposer{}, owned: true, failure: context.DeadlineExceeded}
	group := &ShardGroup{Engine: &forwardOwnedTestEngine{inner: p}, Store: st}
	proxy := NewLeaderProxyForShardGroup(group, WithMigrationRequestAdmission(st, 7))
	group.Txn = proxy
	t.Cleanup(proxy.tm.Close)
	ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), ownedProposalTestKey{}, "receiver"), time.Second)
	defer cancel()
	response, err := group.ForwardTransaction(7).Commit(ctx, []*pb.Request{{Ts: 100, Mutations: []*pb.Mutation{{Key: []byte("key")}}}})
	require.Nil(t, response)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, "receiver", p.seen.Value(ownedProposalTestKey{}))
	deadline, ok := p.seen.Deadline()
	require.True(t, ok)
	expected, _ := ctx.Deadline()
	require.Equal(t, expected, deadline)
	require.Equal(t, int32(1), p.calls.Load(), "unknown owned mutation is never retried")
}
