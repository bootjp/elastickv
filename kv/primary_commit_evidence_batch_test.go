package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type primaryEvidenceCountingEngine struct {
	*followerProxyEngine
	reads int
}

func (*primaryEvidenceCountingEngine) State() raftengine.State { return raftengine.StateLeader }

func (e *primaryEvidenceCountingEngine) LinearizableRead(context.Context) (uint64, error) {
	e.reads++
	return 1, nil
}

func TestPrimaryCommitEvidenceBatchFencesEachOwnerOnce(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	first := &primaryEvidenceCountingEngine{followerProxyEngine: &followerProxyEngine{}}
	second := &primaryEvidenceCountingEngine{followerProxyEngine: &followerProxyEngine{}}
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore(), Engine: first},
		2: {Store: store.NewMVCCStore(), Engine: second},
	}
	owners := map[string]uint64{"a": 1, "b": 1, "c": 2}
	for key, groupID := range owners {
		require.NoError(t, groups[groupID].Store.PutAt(ctx, txnCommitKey([]byte(key), 50), encodeTxnCommitRecord(60), 60, 0))
	}
	shards := NewShardStore(routes, groups).WithPartitionResolver(&fakePartitionResolver{routes: owners})
	results, err := shards.PrimaryCommitEvidenceBatch(ctx, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("a")}, 50)
	require.NoError(t, err)
	require.Len(t, results, 4)
	for _, evidence := range results {
		require.Equal(t, PrimaryCommitEvidence{Readable: true, Found: true, CommitTS: 60}, evidence)
	}
	require.Equal(t, 1, first.reads)
	require.Equal(t, 1, second.reads)
	_, err = shards.PrimaryCommitEvidenceBatch(ctx, make([][]byte, MaxPrimaryCommitEvidenceKeys+1), 50)
	require.ErrorIs(t, err, ErrInvalidRequest)
	require.Equal(t, 1, first.reads)
	require.Equal(t, 1, second.reads)
}

func TestPrimaryCommitEvidenceBatchHonorsSharedDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	raft := &primaryEvidenceBarrierEngine{followerProxyEngine: &followerProxyEngine{}, entered: make(chan struct{}), applied: make(chan struct{})}
	shards := NewShardStore(routes, map[uint64]*ShardGroup{1: {Store: store.NewMVCCStore(), Engine: raft}})
	_, err := shards.PrimaryCommitEvidenceBatch(ctx, [][]byte{[]byte("a"), []byte("b")}, 50)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
