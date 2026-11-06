package etcd

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func identityElectionEngine(t *testing.T, id uint64) *Engine {
	t.Helper()
	storage := etcdraft.NewMemoryStorage()
	require.NoError(t, storage.ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
		Index: proto.Uint64(1), Term: proto.Uint64(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}},
	}}))
	storage.SetHardState(&raftpb.HardState{Term: proto.Uint64(1), Commit: proto.Uint64(1)})
	e := &Engine{nodeID: id, storage: storage, rawNode: mustRawNode(t, storage, id), applied: 1, leaderReady: make(chan struct{}),
		fsm: &testStateMachine{}, recoveredIdentity: identityFixture(), voterCount: 3,
		transport: NewGRPCTransport(nil), peers: map[uint64]Peer{}, peerDispatchers: map[uint64]*peerQueues{},
		pendingProposals: map[uint64]proposalRequest{}, pendingReads: map[uint64]readRequest{}, pendingConfigs: map[uint64]adminRequest{}}
	t.Cleanup(func() { require.NoError(t, e.transport.Close()) })
	e.configIndex.Store(1)
	for peer := uint64(1); peer <= 3; peer++ {
		e.peers[peer] = Peer{NodeID: peer, ID: fmt.Sprint(peer), Address: fmt.Sprintf("127.0.0.1:%d", 7000+peer)}
		if peer != id {
			e.peerDispatchers[peer] = &peerQueues{normal: make(chan dispatchRequest, 128), heartbeat: make(chan dispatchRequest, 128), readIndexResp: make(chan dispatchRequest, 128)}
		}
	}
	e.refreshStatus()
	return e
}

func TestIdentityReconciliationAllowsProtectedElection(t *testing.T) {
	nodes := map[uint64]*Engine{}
	ready := map[uint64]bool{}
	calls := map[uint64]int{}
	for id := uint64(1); id <= 3; id++ {
		e := identityElectionEngine(t, id)
		e.admissionChanged = func() { ready[id] = false }
		e.recoveredAdmission = func(state RecoveredState) error {
			require.False(t, ready[id])
			require.GreaterOrEqual(t, state.AppliedIndex(), state.HardState().GetCommit())
			ready[id] = true // models IdentityReady only, never Control/Data
			calls[id]++
			return nil
		}
		require.NoError(t, e.reconcileIdentityAdmission())
		nodes[id] = e
	}
	require.NoError(t, nodes[1].rawNode.Campaign())
	for round := 0; round < 20; round++ {
		for id, e := range nodes {
			require.NoError(t, e.drainReady())
			for target, queues := range e.peerDispatchers {
				for _, queue := range []chan dispatchRequest{queues.normal, queues.heartbeat, queues.readIndexResp} {
					for len(queue) > 0 {
						require.True(t, ready[id], "dispatch before reconciliation")
						req := <-queue
						require.NoError(t, nodes[target].rawNode.Step(&req.msg))
						require.NoError(t, req.Close())
					}
				}
			}
		}
	}
	require.Equal(t, raftengine.StateLeader, nodes[1].State())
	for id, e := range nodes {
		require.True(t, ready[id])
		require.Greater(t, calls[id], 1, "term/role changes must revalidate")
		require.Equal(t, uint64(2), e.status.Term)
	}
}

func TestIdentityReconciliationBlocksDispatchUntilCallbackCompletes(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprint(reject), func(t *testing.T) {
			e := identityElectionEngine(t, 1)
			e.admissionChanged = func() {}
			entered, release := make(chan struct{}), make(chan struct{})
			denied := errors.New("policy revalidation failed")
			e.recoveredAdmission = func(RecoveredState) error {
				close(entered)
				<-release
				if reject {
					return denied
				}
				return nil
			}
			require.NoError(t, e.rawNode.Campaign())
			done := make(chan error, 1)
			go func() { done <- e.drainReady() }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("reconciliation not reached")
			}
			assertIdentityDispatchEmpty(t, e)
			close(release)
			select {
			case err := <-done:
				if reject {
					require.ErrorIs(t, err, denied)
				} else {
					require.NoError(t, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("drain did not complete")
			}
			if reject {
				assertIdentityDispatchEmpty(t, e)
				require.True(t, e.admissionDirty)
			} else {
				require.NotEmpty(t, e.peerDispatchers[2].heartbeat)
			}
		})
	}
}

func assertIdentityDispatchEmpty(t *testing.T, e *Engine) {
	t.Helper()
	for _, q := range e.peerDispatchers {
		require.Empty(t, q.normal)
		require.Empty(t, q.heartbeat)
	}
}
