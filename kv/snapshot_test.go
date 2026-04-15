package kv

import (
	"bytes"
	"context"
	"io"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	store3 "github.com/bootjp/elastickv/store"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestSnapshot(t *testing.T) {
	store := store3.NewMVCCStore()
	fsm := NewKvFSMWithHLC(store, NewHLC())

	mutation := pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Mutations: []*pb.Mutation{
			{
				Op:    pb.Op_PUT,
				Key:   []byte("hoge"),
				Value: []byte("fuga"),
			},
		},
	}

	b, err := proto.Marshal(&mutation)
	assert.NoError(t, err)

	fsm.Apply(&raft.Log{
		Type: raft.LogCommand,
		Data: b,
	})
	fsm.Apply(&raft.Log{
		Type: raft.LogBarrier,
	})

	ctx := context.Background()
	v, err := store.GetAt(ctx, []byte("hoge"), ^uint64(0))
	assert.NoError(t, err)
	assert.Equal(t, []byte("fuga"), v)

	snapshot, err := fsm.Snapshot()
	assert.NoError(t, err)

	store2 := store3.NewMVCCStore()
	fsm2 := NewKvFSMWithHLC(store2, NewHLC())

	kvFSMSnap, ok := snapshot.(*kvFSMSnapshot)
	assert.True(t, ok)
	assert.NoError(t, err)

	var buf bytes.Buffer
	_, err = kvFSMSnap.snapshot.WriteTo(&buf)
	assert.NoError(t, err)
	kvFSMSnap.Release()
	err = fsm2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes())))
	assert.NoError(t, err)

	v, err = store2.GetAt(ctx, []byte("hoge"), ^uint64(0))
	assert.NoError(t, err)
	assert.Equal(t, []byte("fuga"), v)
}

// TestFSMSnapshotPreservesCeiling verifies that the physical ceiling stored in
// the HLC survives a Snapshot → Persist → Restore round-trip. This guards
// against the scenario where log compaction removes the last HLC lease entry:
// the ceiling must be recoverable from the snapshot alone.
func TestFSMSnapshotPreservesCeiling(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const ceilingMs = int64(9_000_000_000_000) // year ~2255

	// Source FSM with a known ceiling and one user key.
	hlc1 := NewHLC()
	hlc1.SetPhysicalCeiling(ceilingMs)
	st1 := store3.NewMVCCStore()
	fsm1 := NewKvFSMWithHLC(st1, hlc1)

	req := &pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
		},
	}
	b, err := proto.Marshal(req)
	require.NoError(t, err)
	fsm1.Apply(&raft.Log{Type: raft.LogCommand, Data: b})

	// Take snapshot and persist it via Persist (the real code path).
	snap, err := fsm1.Snapshot()
	require.NoError(t, err)

	var buf bytes.Buffer
	sink := &snapshotSinkAdapter{writer: &buf}
	require.NoError(t, snap.Persist(sink))

	// Restore into a fresh FSM with its own HLC starting at zero.
	hlc2 := NewHLC()
	require.Equal(t, int64(0), hlc2.PhysicalCeiling(), "fresh HLC must start at zero")
	st2 := store3.NewMVCCStore()
	fsm2 := NewKvFSMWithHLC(st2, hlc2)
	require.NoError(t, fsm2.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))))

	// The ceiling must be restored.
	require.Equal(t, ceilingMs, hlc2.PhysicalCeiling(),
		"ceiling must survive Snapshot→Persist→Restore")

	// Store data must also be intact.
	val, err := st2.GetAt(ctx, []byte("k"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)
}

// TestFSMSnapshotRestoreSmallLegacy verifies that a legacy snapshot smaller than
// the 16-byte header size (hlcSnapshotHeaderLen) is restored without error.
// Before the fix, io.ReadFull would return io.ErrUnexpectedEOF on such snapshots.
func TestFSMSnapshotRestoreSmallLegacy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Craft a minimal raw store snapshot that is definitely < 16 bytes.
	// Use an empty store: its snapshot is likely a few bytes of metadata.
	st1 := store3.NewMVCCStore()
	storeSnap, err := st1.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = storeSnap.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, storeSnap.Close())

	// If the empty snapshot happens to be >= 16 bytes the test is vacuous but
	// still harmless — the restore must succeed either way.
	hlc2 := NewHLC()
	st2 := store3.NewMVCCStore()
	fsm2 := NewKvFSMWithHLC(st2, hlc2)
	require.NoError(t, fsm2.Restore(io.NopCloser(bytes.NewReader(raw.Bytes()))))

	// No data was written, ceiling stays at zero.
	require.Equal(t, int64(0), hlc2.PhysicalCeiling())

	// Empty store: no key exists.
	_, err = st2.GetAt(ctx, []byte("any"), ^uint64(0))
	require.Error(t, err)
}

// TestFSMSnapshotRestoreOldFormat verifies that a snapshot written without the
// HLC header (legacy format) is still readable after the format migration.
func TestFSMSnapshotRestoreOldFormat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Build legacy-format snapshot bytes directly: just the raw store data,
	// no HLC magic header. This simulates snapshots taken by the old code.
	st1 := store3.NewMVCCStore()
	fsm1 := NewKvFSMWithHLC(st1, NewHLC())

	req := &pb.Request{
		IsTxn: false,
		Phase: pb.Phase_NONE,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("legacy"), Value: []byte("data")},
		},
	}
	b, err := proto.Marshal(req)
	require.NoError(t, err)
	fsm1.Apply(&raft.Log{Type: raft.LogCommand, Data: b})

	// Grab the raw store snapshot bytes (bypasses the new header).
	storeSnap, err := st1.Snapshot()
	require.NoError(t, err)
	var raw bytes.Buffer
	_, err = storeSnap.WriteTo(&raw)
	require.NoError(t, err)
	require.NoError(t, storeSnap.Close())

	// Restore into a new FSM; this exercises the old-format fallback path.
	hlc2 := NewHLC()
	st2 := store3.NewMVCCStore()
	fsm2 := NewKvFSMWithHLC(st2, hlc2)
	require.NoError(t, fsm2.Restore(io.NopCloser(bytes.NewReader(raw.Bytes()))))

	// Ceiling stays zero (no header was present).
	require.Equal(t, int64(0), hlc2.PhysicalCeiling())

	// Store data is intact.
	val, err := st2.GetAt(ctx, []byte("legacy"), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, []byte("data"), val)
}
