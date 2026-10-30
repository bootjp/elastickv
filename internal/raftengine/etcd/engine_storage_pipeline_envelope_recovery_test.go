package etcd

import (
	"bufio"
	"bytes"
	"context"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestEnginePipelineNativeRaftEnvelopeQuorumSnapshotCleanReopen(t *testing.T) {
	c := newEnvelopeNativeCluster(t, false)
	baseline := requireAsyncNativeWrite(t, c.write([]byte("retained-left"), []byte("baseline"), 100))
	c.wait(t, func() bool {
		for _, node := range c.nodes {
			if node.engine.AppliedIndex() < baseline {
				return false
			}
		}
		return true
	}, "encrypted baseline on all three voters")
	c.partition3.Store(true)
	c.disks[1].armed.Store(true)
	secret := []byte("raft-envelope-native-secret-not-plaintext-in-WAL-or-snapshot")
	done := c.write([]byte("envelope-secret"), secret, 200)
	index := testutil.Recv(t, c.disks[1].entered, 30*time.Second, "encrypted follower entry never reached real Sync")
	require.Greater(t, index, baseline)
	c.wait(t, func() bool { return c.disks[0].synced.Load() >= index }, "encrypted leader WAL Sync")
	c.heldHeartbeatWitness(t, done, baseline, index, c.nodes[0].engine.Status().Term)
	for _, st := range c.stores {
		_, err := st.GetAt(t.Context(), []byte("envelope-secret"), 200)
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
	c.disks[1].unblock()
	require.Equal(t, index, requireAsyncNativeWrite(t, done))
	c.wait(t, func() bool { return c.nodes[1].engine.AppliedIndex() >= index }, "encrypted quorum apply")
	require.False(t, c.earlyAck.Load())
	for _, node := range c.nodes[:2] {
		entries, err := node.engine.storage.Entries(index, index+1, math.MaxUint64)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		requireEnvelopeNativeEntry(t, c.cipher, entries[0], []byte("envelope-secret"), secret)
	}
	c.fsms[0].checkpoint.Store(true)
	checkpoint := requireAsyncNativeWrite(t, c.write([]byte("envelope-checkpoint"), []byte("checkpoint"), 201))
	c.wait(t, func() bool { first, _ := c.nodes[0].engine.storage.FirstIndex(); return first > checkpoint }, "encrypted local checkpoint compaction")
	c.fsms[0].checkpoint.Store(false)
	c.partition3.Store(false)
	c.wait(t, func() bool { return c.nodes[2].engine.AppliedIndex() == checkpoint && c.fsms[2].restores.Load() > 0 }, "encrypted streamed snapshot restore")
	require.Positive(t, c.nodes[0].engine.SnapshotStreamSendCount())
	requireEnvelopeNativeValue(t, c.stores[2], "envelope-secret", secret, 200)
	requireAsyncNativeMembershipAndIndex(t, c.nodes[2].engine, c.fsms[2], c.stores[2], checkpoint)

	// Deliberately leave a committed encrypted tail outside the selected snapshot.
	tailValue := []byte("encrypted-tail-only-recoverable-by-real-Raft-WAL-replay")
	tail := requireAsyncNativeWrite(t, c.write([]byte("envelope-tail"), tailValue, 202))
	c.wait(t, func() bool { return c.nodes[2].engine.AppliedIndex() == tail }, "encrypted tail on restored voter")
	for _, node := range c.nodes {
		closeEnvelopeNativeEngine(t, node.engine, false)
	}
	selected := requireEnvelopeNativeDiskPrefix(t, c, checkpoint, tail, secret, tailValue)
	requireEnvelopeNativeCleanReopen(t, c, selected, tail, secret, tailValue)
	requireEnvelopeNativeReopenRefusesMissingCipher(t, c, selected, tail, secret)
	t.Logf("active raft envelope: quorum=%d selected_snapshot=%d encrypted_tail=%d; clean reopen only", index, checkpoint, tail)
}

func requireEnvelopeNativeEntry(t *testing.T, cipher *encryption.Cipher, entry *raftpb.Entry, key, value []byte) {
	t.Helper()
	require.Equal(t, raftpb.EntryNormal, entry.GetType())
	require.Greater(t, entry.GetIndex(), uint64(1))
	_, wrapped, ok := decodeProposalEnvelope(entry.GetData())
	require.True(t, ok)
	require.False(t, bytes.Contains(wrapped, value), "Raft entry must not downgrade to plaintext")
	envelope, err := encryption.DecodeEnvelope(wrapped)
	require.NoError(t, err)
	require.Equal(t, envelopeNativeRaftKeyID, envelope.KeyID)
	plain, err := encryption.UnwrapRaftPayload(cipher, wrapped)
	require.NoError(t, err)
	// TransactionManager prefixes a single Request protobuf with format byte 0.
	require.NotEmpty(t, plain)
	require.Equal(t, byte(0), plain[0])
	var request pb.Request
	require.NoError(t, proto.Unmarshal(plain[1:], &request))
	require.Len(t, request.GetMutations(), 1)
	mutation := request.GetMutations()[0]
	require.Equal(t, pb.Op_PUT, mutation.GetOp())
	require.Equal(t, key, mutation.GetKey())
	require.Equal(t, value, mutation.GetValue())
}

func requireEnvelopeNativeValue(t *testing.T, st store.MVCCStore, key string, want []byte, timestamp uint64) {
	t.Helper()
	got, err := st.GetAt(t.Context(), []byte(key), timestamp)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func requireEnvelopeNativeDiskPrefix(t *testing.T, c *envelopeNativeCluster, checkpoint, tail uint64, secret, tailValue []byte) raftpb.Snapshot {
	t.Helper()
	node := c.nodes[2]
	selected, err := readWALRestoreState(zap.NewNop(), filepath.Join(node.dir, walDirName), filepath.Join(node.dir, snapDirName))
	require.NoError(t, err)
	defer func() { require.NoError(t, selected.w.Close()) }()
	require.Equal(t, checkpoint, selected.snapshot.GetMetadata().GetIndex())
	require.Equal(t, []uint64{1, 2, 3}, selected.snapshot.GetMetadata().GetConfState().GetVoters())
	require.Empty(t, selected.snapshot.GetMetadata().GetConfState().GetLearners())
	require.GreaterOrEqual(t, selected.hardState.GetCommit(), tail)
	var found bool
	for i := range selected.entries {
		entry := &selected.entries[i]
		if entry.GetIndex() == tail {
			requireEnvelopeNativeEntry(t, c.cipher, entry, []byte("envelope-tail"), tailValue)
			found = true
		}
	}
	require.True(t, found, "actual selected WAL must retain the encrypted committed tail")
	token, err := decodeSnapshotToken(selected.snapshot.GetData())
	require.NoError(t, err)
	require.Equal(t, checkpoint, token.Index)
	path := fsmSnapPath(filepath.Join(node.dir, fsmSnapDirName), token.Index)
	require.NoError(t, verifyFSMSnapshotFile(path, token.CRC32C))
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	_, cutover, err := kv.ReadSnapshotHeader(bufio.NewReader(bytes.NewReader(raw)))
	require.NoError(t, err)
	require.Equal(t, uint64(1), cutover, "physical KV snapshot must preserve the active envelope regime")
	require.False(t, bytes.Contains(raw, secret), "physical snapshot must retain storage encryption")
	require.False(t, bytes.Contains(raw, tailValue), "snapshot must not contain the later WAL-only value")
	return selected.snapshot
}

func requireEnvelopeNativeCleanReopen(t *testing.T, c *envelopeNativeCluster, selected raftpb.Snapshot, tail uint64, secret, tailValue []byte) {
	t.Helper()
	observer := &asyncNativeEncryptionObserver{}
	fresh, fsm := newEnvelopeNativeFSM(t, c.cipher, 3, 2, observer)
	t.Cleanup(func() { require.NoError(t, fresh.Close()) })
	node := &transportTestNode{peer: c.nodes[2].peer, dir: c.nodes[2].dir, transport: NewGRPCTransport(c.peers)}
	disk := &envelopeNativeDisk{&asyncNativeAcceptanceDisk{entered: make(chan uint64, 1), release: make(chan struct{})}}
	e := openEnvelopeNativeEngine(t, node, nil, fsm, disk, false, c.cipher, context.Background(), nil)
	t.Cleanup(func() { disk.unblock(); closeEnvelopeNativeEngine(t, e, false) })
	testutil.Closed(t, e.startedCh, 30*time.Second, "encrypted explicit async clean reopen")
	require.NotNil(t, e.storagePipeline)
	require.Nil(t, e.snapshotReqCh)
	require.Equal(t, tail, e.AppliedIndex())
	requireEnvelopeNativeValue(t, fresh, "envelope-secret", secret, 200)
	requireEnvelopeNativeValue(t, fresh, "envelope-tail", tailValue, 202)
	requireAsyncNativeMembershipAndIndex(t, e, fsm, fresh, tail)
	require.Positive(t, fsm.restores.Load())
	installed, err := e.storage.Snapshot()
	require.NoError(t, err)
	require.True(t, proto.Equal(&selected, installed), "reopen must select the same exact Raft snapshot tuple")
	require.Zero(t, observer.failures.Load())
	closeEnvelopeNativeEngine(t, e, false)
}

func requireEnvelopeNativeReopenRefusesMissingCipher(t *testing.T, c *envelopeNativeCluster, selected raftpb.Snapshot, tail uint64, secret []byte) {
	t.Helper()
	fresh, fsm := newEnvelopeNativeFSM(t, c.cipher, 3, 3, &asyncNativeEncryptionObserver{})
	t.Cleanup(func() { require.NoError(t, fresh.Close()) })
	node := &transportTestNode{peer: c.nodes[2].peer, dir: c.nodes[2].dir, transport: NewGRPCTransport(c.peers)}
	disk := &envelopeNativeDisk{&asyncNativeAcceptanceDisk{entered: make(chan uint64, 1), release: make(chan struct{})}}
	e := openEnvelopeNativeEngine(t, node, nil, fsm, disk, false, nil, context.Background(), nil)
	t.Cleanup(func() { disk.unblock(); closeEnvelopeNativeEngine(t, e, true) })
	testutil.Closed(t, e.Done(), 30*time.Second, "missing-cipher WAL replay must halt, not silently downgrade")
	require.True(t, errors.Is(e.Err(), ErrRaftUnwrapFailed), "unexpected replay failure: %v", e.Err())
	require.Equal(t, selected.GetMetadata().GetIndex(), e.AppliedIndex())
	require.Less(t, e.AppliedIndex(), tail)
	requireEnvelopeNativeValue(t, fresh, "envelope-secret", secret, 200)
	_, err := fresh.GetAt(t.Context(), []byte("envelope-tail"), 202)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}
