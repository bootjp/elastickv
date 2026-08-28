package etcd

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestGRPCSnapshotTransportRoundTripsSSTIngestPayload(t *testing.T) {
	const (
		index     = uint64(321)
		term      = uint64(7)
		chunkSize = 17
	)

	srcStore, err := store.NewPebbleStore(
		filepath.Join(t.TempDir(), "src"),
		store.WithSSTIngestSnapshots(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, srcStore.Close()) })
	require.NoError(t, srcStore.PutAt(context.Background(), []byte("key"), []byte("value"), 41, 0))

	senderFSM := kv.NewKvFSMWithHLC(srcStore, kv.NewHLC())
	snapshot, err := senderFSM.Snapshot()
	require.NoError(t, err)
	var payload bytes.Buffer
	_, err = snapshot.WriteTo(&payload)
	require.NoError(t, err)
	require.NoError(t, snapshot.Close())

	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(index, term, nil),
		},
	}
	encodedMetadata, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	chunks := []*pb.EtcdRaftSnapshotChunk{{Metadata: encodedMetadata}}
	for remaining := payload.Bytes(); len(remaining) > 0; {
		n := min(chunkSize, len(remaining))
		chunks = append(chunks, &pb.EtcdRaftSnapshotChunk{Chunk: bytes.Clone(remaining[:n])})
		remaining = remaining[n:]
	}
	chunks[len(chunks)-1].Final = true

	fsmSnapDir := t.TempDir()
	transport := NewGRPCTransport(nil)
	transport.SetSpoolDir(t.TempDir())
	transport.SetFSMSnapDir(fsmSnapDir)
	received, err := transport.receiveSnapshotStream(&testSendSnapshotServer{chunks: chunks})
	require.NoError(t, err)
	require.True(t, isSnapshotToken(received.Snapshot.Data))

	dstStore, err := store.NewPebbleStore(filepath.Join(t.TempDir(), "dst"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dstStore.Close()) })
	receiverFSM := kv.NewKvFSMWithHLC(dstStore, kv.NewHLC())
	_, err = restoreSnapshotState(receiverFSM, *received.Snapshot, index, fsmSnapDir, nil, nil)
	require.NoError(t, err)

	value, err := dstStore.GetAt(context.Background(), []byte("key"), 41)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}
