package etcd

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type asyncNativeAcceptanceWrite struct {
	response *kv.TransactionResponse
	err      error
}

func (c *asyncNativeAcceptanceCluster) write(key, value []byte, timestamp uint64) <-chan asyncNativeAcceptanceWrite {
	done := make(chan asyncNativeAcceptanceWrite, 1)
	c.proposals.Add(1)
	go func() {
		defer c.proposals.Done()
		ctx, cancel := context.WithTimeout(c.ctx, 30*time.Second)
		defer cancel()
		response, err := kv.NewLeaderProxyWithEngine(c.nodes[0].engine).Commit(ctx, []*pb.Request{{Ts: timestamp,
			Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: key, Value: value}},
		}})
		done <- asyncNativeAcceptanceWrite{response: response, err: err}
	}()
	return done
}

func requireAsyncNativeWrite(t *testing.T, done <-chan asyncNativeAcceptanceWrite) uint64 {
	t.Helper()
	result := testutil.Recv(t, done, 30*time.Second, "actual RawKV-compatible proposal did not complete")
	require.NoError(t, result.err)
	require.NotNil(t, result.response)
	require.NotZero(t, result.response.CommitIndex)
	return result.response.CommitIndex
}

func (c *asyncNativeAcceptanceCluster) requireHeldQuorum(t *testing.T, done <-chan asyncNativeAcceptanceWrite, before, target, term uint64) {
	t.Helper()
	require.False(t, c.earlyAck.Load(), "follower emitted an append ACK before its required Sync returned")
	require.Less(t, c.ack2.Load(), target)
	require.Less(t, c.disks[1].synced.Load(), target)
	for _, i := range []int{0, 1} {
		e := c.nodes[i].engine
		require.Equal(t, term, e.Status().Term, "heartbeat progress must not hide a leader/term change")
		require.Equal(t, before, e.Status().CommitIndex, "one durable voter is not a committed quorum")
	}
	for i, node := range c.nodes {
		e := node.engine
		require.Equal(t, before, e.AppliedIndex(), "uncommitted large entry advanced the FSM")
		_, err := c.stores[i].GetAt(t.Context(), []byte("large-raw-value"), 200)
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
	select {
	case result := <-done:
		t.Fatalf("client completed before durable quorum: %+v", result)
	default:
	}
}

func (c *asyncNativeAcceptanceCluster) heldHeartbeatWitness(t *testing.T, done <-chan asyncNativeAcceptanceWrite, before, target, term uint64) {
	t.Helper()
	for draining := true; draining; {
		select {
		case <-c.events:
		default:
			draining = false
		}
	}
	nonce := []byte(fmt.Sprintf("fresh-read-after-held-sync-%d", target))
	require.NoError(t, c.nodes[1].engine.handleTransportMessage(c.ctx, raftpb.Message{
		Type: messageTypePtr(raftpb.MsgReadIndex), From: proto.Uint64(2), To: proto.Uint64(2),
		Entries: []*raftpb.Entry{{Data: nonce}},
	}))
	progress := asyncStorageHeartbeatProgress{nonce: nonce}
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for progress.periodic < 2 {
		select {
		case message := <-c.events:
			progress.observe(message)
			c.requireHeldQuorum(t, done, before, target, term)
		case <-deadline.C:
			t.Fatalf("held disk stopped fresh native heartbeat witness: round=%d nonce=%t ticks=%d", progress.round, progress.confirmed, progress.periodic)
		}
	}
	require.Zero(t, c.dropped.Load())
	t.Logf("held actual Sync: index=%d fresh-round=%d matching-nonce=%t post-confirmation-ticks=%d", target, progress.round, progress.confirmed, progress.periodic)
}

// A single bounded acceptance slice: actual async nodes, real WAL and loopback
// transport, encrypted storage, durable-quorum gating, streamed catch-up, reopen.
// This does not activate async Open or prove full ingress/debt/heap accounting.
func TestEnginePipelineNativeEncryptedLargeQuorumSnapshotRestart(t *testing.T) {
	c := newAsyncNativeAcceptanceCluster(t)
	baseline := requireAsyncNativeWrite(t, c.write([]byte("retained-left"), []byte("baseline"), 100))
	c.wait(t, func() bool {
		for _, node := range c.nodes {
			if node.engine.AppliedIndex() < baseline {
				return false
			}
		}
		return true
	}, "durable baseline on all three voters")
	term := c.nodes[0].engine.Status().Term
	c.partition3.Store(true)
	c.disks[1].armed.Store(true)
	value := make([]byte, asyncNativeLargeValueBytes)
	_, err := rand.New(rand.NewSource(1)).Read(value)
	require.NoError(t, err)
	digest := sha256.Sum256(value)
	done := c.write([]byte("large-raw-value"), value, 200)
	index := testutil.Recv(t, c.disks[1].entered, 30*time.Second, "follower never reached actual large-entry Sync")
	require.Greater(t, index, baseline)
	c.wait(t, func() bool { return c.disks[0].synced.Load() >= index }, "leader's own large-entry WAL Sync")
	c.requireHeldQuorum(t, done, baseline, index, term)
	c.heldHeartbeatWitness(t, done, baseline, index, term)
	c.disks[1].unblock()
	require.Equal(t, index, requireAsyncNativeWrite(t, done))
	c.wait(t, func() bool { return c.nodes[1].engine.AppliedIndex() >= index && c.ack2.Load() >= index }, "actual follower ACK and apply")
	require.False(t, c.earlyAck.Load())
	entries, err := c.nodes[0].engine.storage.Entries(index, index+1, math.MaxUint64)
	require.NoError(t, err)
	require.Len(t, entries, 1, "the large request must remain one Raft entry")
	require.Greater(t, len(entries[0].GetData()), 64_000_000, "exercise the native WAL segment threshold")
	require.Less(t, len(entries[0].GetData()), 64<<20, "leave actual protobuf/proposal wrapper room within the RawKV contract")
	for _, i := range []int{0, 1} {
		requireAsyncNativeValue(t, c.stores[i], digest)
		require.GreaterOrEqual(t, c.observers[i].maximum.Load(), int64(asyncNativeLargeValueBytes), "the real large value must traverse storage encryption")
		require.Zero(t, c.observers[i].failures.Load())
	}
	t.Logf("actual encrypted quorum commit: value_bytes=%d raft_entry_bytes=%d index=%d", len(value), len(entries[0].GetData()), index)
	c.fsms[0].checkpoint.Store(true)
	checkpoint := requireAsyncNativeWrite(t, c.write([]byte("snapshot-marker"), []byte("after-large-value"), 201))
	c.wait(t, func() bool { first, _ := c.nodes[0].engine.storage.FirstIndex(); return first > checkpoint }, "actual checkpoint and log compaction")
	c.partition3.Store(false)
	c.wait(t, func() bool { return c.nodes[2].engine.AppliedIndex() >= checkpoint && c.fsms[2].restores.Load() > 0 }, "real streamed snapshot catch-up")
	require.Positive(t, c.nodes[0].engine.SnapshotStreamSendCount())
	requireAsyncNativeValue(t, c.stores[2], digest)
	requireAsyncNativeMembershipAndIndex(t, c.nodes[2].engine, c.fsms[2], c.stores[2], checkpoint)
	for _, node := range c.nodes {
		require.NoError(t, node.engine.Close())
	}
	selected, err := loadPersistedSnapshot(zap.NewNop(), filepath.Join(c.nodes[2].dir, walDirName), snap.New(zap.NewNop(), filepath.Join(c.nodes[2].dir, snapDirName)))
	require.NoError(t, err)
	require.GreaterOrEqual(t, selected.GetMetadata().GetIndex(), checkpoint)
	require.Equal(t, []uint64{1, 2, 3}, selected.GetMetadata().GetConfState().GetVoters())
	fresh := newAsyncNativeEncryptedStore(t, t.TempDir(), 3, 2, &asyncNativeEncryptionObserver{})
	t.Cleanup(func() { require.NoError(t, fresh.Close()) })
	machine, ok := kv.NewKvFSMWithHLC(fresh, nil).(pipelineNativeConfigurationStateMachine)
	require.True(t, ok)
	fsm := &asyncNativeAcceptanceFSM{pipelineNativeConfigurationStateMachine: machine}
	node := &transportTestNode{peer: c.nodes[2].peer, dir: c.nodes[2].dir, transport: NewGRPCTransport(c.peers)}
	disk := &asyncNativeAcceptanceDisk{entered: make(chan uint64, 1), release: make(chan struct{})}
	e := openAsyncNativeAcceptanceEngine(t, node, nil, fsm, disk, false, context.Background(), nil)
	t.Cleanup(func() { disk.unblock(); require.NoError(t, e.Close()) })
	testutil.Closed(t, e.startedCh, 30*time.Second, "restarted explicit async engine did not finish replay")
	require.NotNil(t, e.storagePipeline)
	requireAsyncNativeValue(t, fresh, digest)
	requireAsyncNativeMembershipAndIndex(t, e, fsm, fresh, checkpoint)
	require.Positive(t, fsm.restores.Load(), "fresh FSM must restore the selected native snapshot")
	t.Logf("snapshot and async restart: checkpoint=%d restored_applied=%d voters=3", checkpoint, e.AppliedIndex())
}

func requireAsyncNativeValue(t *testing.T, st store.MVCCStore, digest [sha256.Size]byte) {
	t.Helper()
	value, err := st.GetAt(t.Context(), []byte("large-raw-value"), 200)
	require.NoError(t, err)
	require.Len(t, value, asyncNativeLargeValueBytes)
	require.Equal(t, digest, sha256.Sum256(value))
	baseline, err := st.GetAt(t.Context(), []byte("retained-left"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("baseline"), baseline)
}

func requireAsyncNativeMembershipAndIndex(t *testing.T, e *Engine, fsm *asyncNativeAcceptanceFSM, st store.MVCCStore, minimum uint64) {
	t.Helper()
	require.GreaterOrEqual(t, e.AppliedIndex(), minimum)
	durable, present, err := fsm.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, present)
	require.GreaterOrEqual(t, durable, minimum)
	require.Equal(t, durable, e.AppliedIndex(), "durable FSM prefix and published completed prefix disagree")
	configuration, err := e.Configuration(t.Context())
	require.NoError(t, err)
	require.Len(t, configuration.Servers, 3)
	ids := make([]string, 0, len(configuration.Servers))
	for _, server := range configuration.Servers {
		require.Equal(t, "voter", server.Suffrage)
		ids = append(ids, server.ID)
	}
	require.ElementsMatch(t, []string{"n1", "n2", "n3"}, ids)
	index, err := kv.MigrationConfigurationIndex(t.Context(), st)
	require.NoError(t, err)
	require.Equal(t, uint64(1), index, "data snapshot prefix must not manufacture a new membership epoch")
}
