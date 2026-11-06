package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSQSPointReadPartitionFamiliesUseOwner(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	resolver := NewSQSPartitionResolver(map[string][]uint64{fifoDelayTestQueue: {2, 3, 4, 5}})
	routed := &fifoDelayRoutedStore{MVCCStore: st, resolver: resolver}
	srv := &SQSServer{store: routed, partitionResolver: resolver}
	for _, key := range [][]byte{
		sqsPartitionedMsgDataKey(fifoDelayTestQueue, 1, 1, "message"),
		sqsPartitionedMsgGroupKey(fifoDelayTestQueue, 1, 1, "group"),
		sqsPartitionedMsgDedupKey(fifoDelayTestQueue, 1, 1, "group", "dedup"),
		sqsPartitionedMsgVisKey(fifoDelayTestQueue, 1, 1, 0, "message"),
		sqsPartitionedMsgByAgeKey(fifoDelayTestQueue, 1, 1, 0, "message"),
	} {
		require.NoError(t, st.PutAt(t.Context(), key, []byte("partition"), 20, 0))
		value, err := srv.getSQSAt(t.Context(), key, 20)
		require.NoError(t, err)
		require.Equal(t, []byte("partition"), value)
	}
	require.Equal(t, 5, routed.gets)
	for _, key := range [][]byte{sqsQueueMetaKey(fifoDelayTestQueue), sqsQueueGenKey(fifoDelayTestQueue), sqsQueueSeqKey(fifoDelayTestQueue)} {
		require.NoError(t, st.PutAt(t.Context(), key, []byte("catalog"), 30, 0))
		value, err := srv.getSQSAt(t.Context(), key, 30)
		require.NoError(t, err)
		require.Equal(t, []byte("catalog"), value)
	}
	require.Equal(t, 5, routed.gets, "catalog metadata, generation and sequence must retain global point routing")
}

func TestSQSPointReadOwnerFailureDoesNotBecomeAbsent(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := &SQSServer{store: st, partitionResolver: NewSQSPartitionResolver(map[string][]uint64{fifoDelayTestQueue: {2}})}
	for _, key := range [][]byte{
		sqsPartitionedMsgDataKey("missing.fifo", 0, 1, "message"),
		sqsPartitionedMsgDataKey(fifoDelayTestQueue, 1, 1, "message"),
		append([]byte(SqsPartitionedMsgDataPrefix), []byte("malformed")...),
	} {
		_, err := srv.getSQSAt(t.Context(), key, 20)
		requireDelayUnavailable(t, err)
		require.NotErrorIs(t, err, store.ErrKeyNotFound)
		require.NotContains(t, err.Error(), string(key))
	}
	key := sqsPartitionedMsgDataKey(fifoDelayTestQueue, 0, 1, "message")
	_, err := srv.getSQSAt(t.Context(), key, 20)
	require.Error(t, err, "a partitioned read cannot silently use a store without group reads")
	require.NotErrorIs(t, err, store.ErrKeyNotFound)
	require.NotContains(t, err.Error(), string(key))
	fault := errors.New("owner storage unavailable")
	srv.store = &sqsPointReadFaultStore{MVCCStore: st, fault: fault}
	_, err = srv.getSQSAt(t.Context(), key, 20)
	require.ErrorIs(t, err, fault)
	require.NotErrorIs(t, err, store.ErrKeyNotFound)
}

type sqsPointReadFaultStore struct {
	store.MVCCStore
	fault error
}

func (s *sqsPointReadFaultStore) GetGroupAt(_ context.Context, _ uint64, _ []byte, _ uint64) ([]byte, error) {
	return nil, s.fault
}
