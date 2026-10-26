package filesystem

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type holdPreparedTxn struct {
	kv.Transactional
	prepared chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (h *holdPreparedTxn) Commit(ctx context.Context, reqs []*pb.Request) (*kv.TransactionResponse, error) {
	response, err := h.Transactional.Commit(ctx, reqs)
	if err == nil && len(reqs) == 1 && reqs[0].Phase == pb.Phase_PREPARE {
		h.once.Do(func() {
			close(h.prepared)
			<-h.release
		})
	}
	return response, err
}

func TestOperationJournalTwoGroupCreateOpenWrite(t *testing.T) {
	routes := distribution.NewEngine()
	require.NoError(t, routes.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte{}, End: []byte("!fs|op|"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("!fs|op|"), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))
	factory := etcdraftengine.NewFactory(etcdraftengine.FactoryConfig{
		TickInterval: 10 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 10,
		MaxSizePerMsg: 1 << 20, MaxInflightMsg: 256,
	})
	groups := make(map[uint64]*kv.ShardGroup, 2)
	for _, gid := range []uint64{1, 2} {
		st := store.NewMVCCStore()
		id := fmt.Sprintf("fs-journal-g%d", gid)
		result, err := factory.Create(raftengine.FactoryConfig{
			LocalID: id, LocalAddress: id, DataDir: t.TempDir(), Bootstrap: true,
			StateMachine: kv.NewKvFSMWithHLC(st, kv.NewHLC()),
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			require.NoError(t, result.Engine.Close())
			if result.Close != nil {
				require.NoError(t, result.Close())
			}
		})
		require.Eventually(t, func() bool { return result.Engine.State() == raftengine.StateLeader },
			5*time.Second, 10*time.Millisecond)
		groups[gid] = &kv.ShardGroup{Engine: result.Engine, Store: st, Txn: kv.NewLeaderProxyWithEngine(result.Engine)}
	}
	shards := kv.NewShardStore(routes, groups)
	coord := kv.NewShardedCoordinator(routes, groups, 1, kv.NewHLC(), shards)
	svc, err := NewService(shards, coord)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	journal, err := NewOperationJournal(svc)
	require.NoError(t, err)
	filesystemID, err := journal.EnsureFilesystem(ctx)
	require.NoError(t, err)
	session, err := journal.RegisterSession(ctx, filesystemID, "benchmark-mount")
	require.NoError(t, err)
	id := OperationID{Filesystem: session.Filesystem, Session: session.Session, Sequence: 1}
	file, err := svc.CreateWithOperation(ctx, id, RootInode, []byte("benchmark.dat"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	createID := id
	id.Sequence++
	fh, err := svc.OpenWithOperation(ctx, id, file.Inode, []byte("benchmark-mount"))
	require.NoError(t, err)
	held := &holdPreparedTxn{Transactional: groups[1].Txn, prepared: make(chan struct{}), release: make(chan struct{})}
	groups[1].Txn = held
	ackDone := make(chan error, 1)
	go func() { ackDone <- journal.Acknowledge(ctx, createID) }()
	select {
	case <-held.prepared:
	case <-time.After(5 * time.Second):
		t.Fatal("acknowledgement did not prepare its session participant")
	}
	id.Sequence++
	type writeResult struct {
		count int
		err   error
	}
	writeDone := make(chan writeResult, 1)
	go func() {
		count, writeErr := svc.WriteWithOperation(ctx, id, file.Inode, 0, []byte("data"))
		writeDone <- writeResult{count, writeErr}
	}()
	select {
	case result := <-writeDone:
		close(held.release)
		t.Fatalf("write returned while the acknowledgement held the prepared session lock: %v", result.err)
	case <-time.After(100 * time.Millisecond):
	}
	close(held.release)
	select {
	case err := <-ackDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("acknowledgement did not complete")
	}
	select {
	case result := <-writeDone:
		require.NoError(t, result.err)
		require.Equal(t, 4, result.count)
	case <-time.After(5 * time.Second):
		t.Fatal("write did not complete after acknowledgement")
	}
	id.Sequence++
	require.NoError(t, svc.ReleaseWithOperation(ctx, id, file.Inode, fh, []byte("benchmark-mount")))
}
