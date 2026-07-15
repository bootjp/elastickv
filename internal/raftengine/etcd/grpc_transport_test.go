package etcd

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestTransportContextAppliesTimeoutWhenUnset(t *testing.T) {
	ctx, cancel := transportContext(context.Background(), 3*time.Second)
	t.Cleanup(cancel)

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.InDelta(t, 3*time.Second, time.Until(deadline), float64(200*time.Millisecond))
}

func TestTransportContextPreservesExistingDeadline(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	ctx, childCancel := transportContext(parent, 3*time.Second)
	t.Cleanup(childCancel)

	parentDeadline, ok := parent.Deadline()
	require.True(t, ok)
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.Equal(t, parentDeadline, deadline)
}

func TestSplitSnapshotMessageReusesClonedPayload(t *testing.T) {
	msg := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		Snapshot: &raftpb.Snapshot{
			Data:     []byte("snapshot"),
			Metadata: testSnapshotMetadata(9, 3, nil),
		},
	}

	header, payload, err := splitSnapshotMessage(msg)
	require.NoError(t, err)
	require.NotEmpty(t, header)
	require.Equal(t, []byte("snapshot"), payload)

	payload[0] = 'S'
	require.Equal(t, byte('S'), msg.Snapshot.Data[0])
}

func TestNewSnapshotSpoolUsesConfiguredDir(t *testing.T) {
	dir := t.TempDir()
	spool, err := newSnapshotSpool(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, spool.Close())
	})

	require.Equal(t, dir, filepath.Dir(spool.path))
	_, err = os.Stat(spool.path)
	require.NoError(t, err)
}

func TestReceiveSnapshotStreamRejectsPrematureEOF(t *testing.T) {
	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(7, 3, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	transport := NewGRPCTransport(nil)
	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{{
			Metadata: raw,
			Chunk:    []byte("partial"),
		}},
	}

	_, err = transport.receiveSnapshotStream(stream)
	require.Error(t, err)
	require.True(t, errors.Is(err, errSnapshotStreamShort))
}

func TestReceiveSnapshotStreamRejectsDuplicateMetadata(t *testing.T) {
	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(7, 3, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	transport := NewGRPCTransport(nil)
	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{
			{Metadata: raw, Chunk: []byte("a")},
			{Metadata: raw, Chunk: []byte("b"), Final: true},
		},
	}

	_, err = transport.receiveSnapshotStream(stream)
	require.Error(t, err)
	require.True(t, errors.Is(err, errSnapshotMetadataDuplicate))
}

// TestReceiveSnapshotStream_StreamingTokenWhenFSMSnapDirSet pins the
// memory-safety win: when the receive transport has fsmSnapDir wired,
// the spool file is renamed to fsmSnapPath(...) and Snapshot.Data is
// the 17-byte EKVT token instead of the materialized payload. The
// spool dir is left empty, the fsm-snap dir holds the .fsm file, and
// the token round-trips through restoreSnapshotState onto a fresh FSM.
//
// Without this path, receiving a 1.5+ GiB snapshot allocates the entire
// payload as []byte for raftpb.Snapshot.Data — on a 2.5-GiB-container
// production node that single allocation OOMs the process before
// RestoreSnapshot ever runs (2026-05-08 incident).
func TestReceiveSnapshotStream_StreamingTokenWhenFSMSnapDirSet(t *testing.T) {
	const index = uint64(123)
	const term = uint64(5)

	// Build a properly framed testStateMachine payload (count + length-prefixed
	// items) so restoreSnapshotState can decode it on the receiver. Three
	// distinct entries mirror the production case where a follower has fallen
	// behind by many log entries and is recovering from snapshot.
	senderFSM := &testStateMachine{}
	for _, e := range [][]byte{[]byte("alpha"), []byte("bravo"), []byte("charlie")} {
		senderFSM.Apply(e)
	}
	snap, err := senderFSM.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snap.Close())
	payload := buf.Bytes()

	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(index, term, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	spoolDir := t.TempDir()
	fsmSnapDir := t.TempDir()

	transport := NewGRPCTransport(nil)
	transport.SetSpoolDir(spoolDir)
	transport.SetFSMSnapDir(fsmSnapDir)

	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{
			{Metadata: raw},
			{Chunk: payload, Final: true},
		},
	}
	msg, err := transport.receiveSnapshotStream(stream)
	require.NoError(t, err)
	require.NotNil(t, msg.Snapshot)

	// Snapshot.Data should now be a 17-byte EKVT token, not the payload.
	require.Len(t, msg.Snapshot.Data, snapshotTokenSize, "payload must NOT be inline; got %d bytes", len(msg.Snapshot.Data))
	require.True(t, isSnapshotToken(msg.Snapshot.Data), "Snapshot.Data is not an EKVT token")
	tok, err := decodeSnapshotToken(msg.Snapshot.Data)
	require.NoError(t, err)
	require.Equal(t, index, tok.Index)

	// Spool dir must be empty: the spool file was renamed into fsmSnapDir,
	// not deleted (which would orphan the data) and not left behind (which
	// would leak disk).
	spoolEntries, err := os.ReadDir(spoolDir)
	require.NoError(t, err)
	require.Empty(t, spoolEntries, "spool dir should not retain the file after FinalizeAsFSMFile")

	// .fsm file exists at the canonical path with the on-disk format
	// openAndRestoreFSMSnapshot expects: payload + 4-byte CRC32C footer.
	fsmPath := fsmSnapPath(fsmSnapDir, index)
	got, err := os.ReadFile(fsmPath)
	require.NoError(t, err)
	require.Equal(t, len(payload)+4, len(got), "missing CRC footer")
	require.Equal(t, payload, got[:len(payload)])

	// Token round-trips through the apply path: restoreSnapshotState reads
	// the .fsm file via the streaming io.Reader interface, never
	// materializing the payload as []byte. Verify the receiver FSM ends up
	// with exactly the entries the sender serialized.
	receiverFSM := &testStateMachine{}
	_, restoreErr := restoreSnapshotState(receiverFSM, *msg.Snapshot, msg.Snapshot.GetMetadata().GetIndex(), fsmSnapDir, nil, nil)
	require.NoError(t, restoreErr)
	require.Equal(t, senderFSM.Applied(), receiverFSM.Applied())
}

func TestReceiveSnapshotStreamPreparesBeforeSpoolCreation(t *testing.T) {
	const index = uint64(124)
	payload := []byte("payload written after cleanup")
	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(index, 1, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	fsmSnapDir := t.TempDir()
	transport := NewGRPCTransport(nil)
	transport.SetSpoolDir(t.TempDir())
	transport.SetFSMSnapDir(fsmSnapDir)
	prepareCalls := 0
	transport.SetFSMSnapshotPrepare(func(got uint64) error {
		prepareCalls++
		require.Equal(t, index, got)
		matches, globErr := filepath.Glob(filepath.Join(fsmSnapDir, snapshotSpoolPattern))
		require.NoError(t, globErr)
		require.Empty(t, matches, "prewrite cleanup must run before creating the receive spool")
		return nil
	})

	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{{
			Metadata: raw,
			Chunk:    payload,
			Final:    true,
		}},
	}

	msg, err := transport.receiveSnapshotStream(stream)
	require.NoError(t, err)
	require.Equal(t, 1, prepareCalls)
	require.True(t, isSnapshotToken(msg.Snapshot.Data))
	require.FileExists(t, fsmSnapPath(fsmSnapDir, index))
}

func TestSendSnapshotProtectsFinalizedFSMFileUntilEngineRelease(t *testing.T) {
	const index = uint64(91)

	senderFSM := &testStateMachine{}
	senderFSM.Apply([]byte("entry-for-protection-test"))
	snap, err := senderFSM.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snap.Close())

	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(index, 1, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	fsmSnapDir := t.TempDir()
	var protected []uint64
	var unprotected []uint64
	transport := NewGRPCTransport(nil)
	transport.SetSpoolDir(t.TempDir())
	transport.SetFSMSnapDir(fsmSnapDir)
	transport.SetFSMSnapshotProtection(
		func(index uint64) bool {
			protected = append(protected, index)
			return true
		},
		func(index uint64) { unprotected = append(unprotected, index) },
	)
	transport.SetHandler(func(_ context.Context, msg raftpb.Message) error {
		require.NotNil(t, msg.Snapshot)
		require.True(t, isSnapshotToken(msg.Snapshot.Data))
		return nil
	})

	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{
			{Metadata: raw},
			{Chunk: buf.Bytes(), Final: true},
		},
	}

	require.NoError(t, transport.SendSnapshot(stream))
	require.Equal(t, []uint64{index}, protected)
	require.Empty(t, unprotected)
	require.FileExists(t, fsmSnapPath(fsmSnapDir, index))
}

func TestDrainSnapshotChunksProtectsBeforePublishingFSMFile(t *testing.T) {
	const index = uint64(125)
	payload := []byte("payload protected before final rename")
	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(index, 1, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	fsmSnapDir := t.TempDir()
	spool, err := newSnapshotSpool(fsmSnapDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, spool.Close())
	})
	var protected []uint64
	protectFn := func(got uint64) bool {
		protected = append(protected, got)
		_, statErr := os.Stat(fsmSnapPath(fsmSnapDir, got))
		require.True(t, os.IsNotExist(statErr), "protection must be registered before the final .fsm path is visible")
		return true
	}
	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{{
			Metadata: raw,
			Chunk:    payload,
			Final:    true,
		}},
	}

	msg, payloadBytes, err := drainSnapshotChunks(stream, spool, fsmSnapDir, func(uint64) error { return nil }, protectFn, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), payloadBytes)
	require.Equal(t, []uint64{index}, protected)
	require.True(t, isSnapshotToken(msg.Snapshot.Data))
	require.FileExists(t, fsmSnapPath(fsmSnapDir, index))
}

func TestDrainSnapshotChunksRejectsStaleFSMProtection(t *testing.T) {
	const index = uint64(127)
	payload := []byte("stale payload must not be published")
	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(index, 1, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	fsmSnapDir := t.TempDir()
	spool, err := newSnapshotSpool(fsmSnapDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, spool.Close())
	})
	var protected []uint64
	var unprotected []uint64
	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{{
			Metadata: raw,
			Chunk:    payload,
			Final:    true,
		}},
	}

	_, payloadBytes, err := drainSnapshotChunks(
		stream,
		spool,
		fsmSnapDir,
		func(uint64) error { return nil },
		func(got uint64) bool {
			protected = append(protected, got)
			return false
		},
		func(got uint64) { unprotected = append(unprotected, got) },
	)
	require.Error(t, err)
	require.ErrorIs(t, err, errReceivedFSMSnapshotStale)
	require.Zero(t, payloadBytes)
	require.Equal(t, []uint64{index}, protected)
	require.Empty(t, unprotected)
	require.NoFileExists(t, fsmSnapPath(fsmSnapDir, index))
}

func TestDrainSnapshotChunksUnprotectsWhenFinalizeFails(t *testing.T) {
	const index = uint64(126)
	payload := []byte("payload whose final rename fails")
	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(index, 1, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	spool, err := newSnapshotSpool(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, spool.Close())
	})
	fsmSnapDir := t.TempDir()
	var protected []uint64
	var unprotected []uint64
	syncErr := errors.New("simulated directory sync failure")
	oldSnapshotSyncDir := snapshotSyncDir
	snapshotSyncDir = func(string) error { return syncErr }
	t.Cleanup(func() {
		snapshotSyncDir = oldSnapshotSyncDir
	})
	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{{
			Metadata: raw,
			Chunk:    payload,
			Final:    true,
		}},
	}

	_, _, err = drainSnapshotChunks(
		stream,
		spool,
		fsmSnapDir,
		func(uint64) error { return nil },
		func(got uint64) bool {
			protected = append(protected, got)
			return true
		},
		func(got uint64) { unprotected = append(unprotected, got) },
	)
	require.Error(t, err)
	require.ErrorIs(t, err, syncErr)
	require.Equal(t, []uint64{index}, protected)
	require.Equal(t, []uint64{index}, unprotected)
}

func TestDrainSnapshotChunksPreparesBeforePayloadWrite(t *testing.T) {
	const index = uint64(124)
	payload := []byte("payload written after prepare")
	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(index, 1, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	fsmSnapDir := t.TempDir()
	spool, err := newSnapshotSpool(fsmSnapDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, spool.Close())
	})

	prepareCalls := 0
	prepareFn := func(got uint64) error {
		prepareCalls++
		require.Equal(t, index, got)
		info, statErr := os.Stat(spool.path)
		require.NoError(t, statErr)
		require.Zero(t, info.Size(), "prepare must run before the first payload byte is spooled")
		return nil
	}
	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{{
			Metadata: raw,
			Chunk:    payload,
			Final:    true,
		}},
	}

	msg, payloadBytes, err := drainSnapshotChunks(stream, spool, fsmSnapDir, prepareFn, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), payloadBytes)
	require.Equal(t, 1, prepareCalls)
	require.True(t, isSnapshotToken(msg.Snapshot.Data))
	got, err := readFSMSnapshotPayload(fsmSnapPath(fsmSnapDir, index))
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestDrainSnapshotChunksRejectsPayloadBeforeMetadata(t *testing.T) {
	fsmSnapDir := t.TempDir()
	spool, err := newSnapshotSpool(fsmSnapDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, spool.Close())
	})

	prepareCalls := 0
	prepareFn := func(uint64) error {
		prepareCalls++
		return nil
	}
	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{{
			Chunk: []byte("payload before metadata"),
			Final: true,
		}},
	}

	_, payloadBytes, err := drainSnapshotChunks(stream, spool, fsmSnapDir, prepareFn, nil, nil)
	require.ErrorIs(t, err, errSnapshotMetadataNil)
	require.Zero(t, payloadBytes)
	require.Zero(t, prepareCalls)
	info, statErr := os.Stat(spool.path)
	require.NoError(t, statErr)
	require.Zero(t, info.Size())
}

// TestReceiveSnapshotStream_SpoolPlacedInFSMSnapDir pins the EXDEV-avoidance
// fix from PR #747 round-3 (Codex P1): when fsmSnapDir is wired, the spool
// file MUST be created inside fsmSnapDir (not spoolDir), so that the
// FinalizeAsFSMFile rename stays within a single filesystem and cannot fail
// with syscall.EXDEV. Without this, an operator who mounts spoolDir and
// fsmSnapDir on separate volumes hits a hard receive failure with the
// leader retrying indefinitely. Standard engine wiring puts both under
// cfg.DataDir but the receive code must not assume that.
func TestReceiveSnapshotStream_SpoolPlacedInFSMSnapDir(t *testing.T) {
	const index = uint64(55)
	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(index, 1, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	// Distinct directories — different absolute paths so we can prove the
	// spool went to fsmSnapDir, not spoolDir.
	spoolDir := t.TempDir()
	fsmSnapDir := t.TempDir()

	// Inject a syncDir failure to keep the partial-state observable: the
	// rename has fired (so the .fsm file is at fsmSnapPath), Finalize
	// returned the syncDir error, and we can inspect both directories.
	original := snapshotSyncDir
	syncErr := errors.New("simulated syncDir failure")
	snapshotSyncDir = func(string) error { return syncErr }
	t.Cleanup(func() { snapshotSyncDir = original })

	transport := NewGRPCTransport(nil)
	transport.SetSpoolDir(spoolDir)
	transport.SetFSMSnapDir(fsmSnapDir)

	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{
			{Metadata: raw},
			{Chunk: []byte("payload-for-spool-placement-test"), Final: true},
		},
	}
	_, err = transport.receiveSnapshotStream(stream)
	require.ErrorIs(t, err, syncErr, "should surface the simulated syncDir error")

	// spoolDir must be empty: the spool was created in fsmSnapDir, not
	// spoolDir. If a future refactor reverts the placement decision, this
	// directory would carry an elastickv-etcd-snapshot-* leftover at
	// minimum, or a renamed .fsm at maximum.
	spoolEntries, err := os.ReadDir(spoolDir)
	require.NoError(t, err)
	require.Empty(t, spoolEntries, "spool dir must NOT receive the spool file when fsmSnapDir is wired (rename would risk EXDEV)")

	// fsmSnapDir holds the renamed .fsm file at the canonical path.
	finalPath := fsmSnapPath(fsmSnapDir, index)
	_, statErr := os.Stat(finalPath)
	require.NoError(t, statErr, "renamed .fsm file should exist at canonical path")
}

// TestSendSnapshot_ApplyFailureRemovesFinalizedFSMFile pins the
// orphan-cleanup behaviour from PR #747 round-4 (Codex P2): when the
// receive path successfully finalizes the snapshot as
// fsmSnapDir/<index>.fsm but the engine handler (t.handle) then fails
// — transient context cancel, closed engine, etc. — the finalized .fsm
// file MUST be removed. Otherwise retries at later snapshot indexes
// accumulate orphan .fsm payloads in fsmSnapDir until startup runs
// cleanupStaleFSMSnaps. Same-index retries are already safe via
// os.Rename's atomic-replace, so this test exercises the cross-index
// case where the orphan would actually persist.
func TestSendSnapshot_ApplyFailureRemovesFinalizedFSMFile(t *testing.T) {
	const index = uint64(77)

	// Build a real testStateMachine payload + framed bytes so receive
	// finalizes a syntactically valid .fsm file.
	senderFSM := &testStateMachine{}
	senderFSM.Apply([]byte("entry-for-orphan-cleanup-test"))
	snap, err := senderFSM.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snap.Close())
	payload := buf.Bytes()

	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(index, 1, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	fsmSnapDir := t.TempDir()

	transport := NewGRPCTransport(nil)
	transport.SetSpoolDir(t.TempDir())
	transport.SetFSMSnapDir(fsmSnapDir)
	var protected []uint64
	var unprotected []uint64
	transport.SetFSMSnapshotProtection(
		func(index uint64) bool {
			protected = append(protected, index)
			return true
		},
		func(index uint64) { unprotected = append(unprotected, index) },
	)

	// Wire a handler that always fails so SendSnapshot exercises the
	// orphan-cleanup branch.
	applyErr := errors.New("simulated apply failure")
	transport.SetHandler(func(_ context.Context, _ raftpb.Message) error {
		return applyErr
	})

	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{
			{Metadata: raw},
			{Chunk: payload, Final: true},
		},
	}

	err = transport.SendSnapshot(stream)
	require.Error(t, err)
	require.ErrorIs(t, err, applyErr, "SendSnapshot must surface the apply failure")
	require.Equal(t, []uint64{index}, protected)
	require.Equal(t, []uint64{index}, unprotected)

	// THE point: the .fsm file at the canonical path MUST have been
	// removed. Without the cleanup, leader retries at later indexes
	// would accumulate one .fsm per failed apply until next startup.
	finalPath := fsmSnapPath(fsmSnapDir, index)
	_, statErr := os.Stat(finalPath)
	require.True(t, os.IsNotExist(statErr),
		"orphan .fsm file at %s must be removed after apply failure (got stat err: %v)", finalPath, statErr)
}

// TestReceiveSnapshotStream_LegacyFallbackWhenNoFSMSnapDir pins the
// behaviour when fsmSnapDir is unset: receive still works, just via the
// pre-PR materialization path. Tests that don't wire an fsmSnapDir (most
// of the existing suite) MUST keep behaving exactly as before this PR.
func TestReceiveSnapshotStream_LegacyFallbackWhenNoFSMSnapDir(t *testing.T) {
	payload := []byte("legacy-inline-payload")
	metadata := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(1),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(42, 1, nil),
		},
	}
	raw, err := proto.Marshal(&metadata)
	require.NoError(t, err)

	transport := NewGRPCTransport(nil)
	transport.SetSpoolDir(t.TempDir())
	// fsmSnapDir intentionally unset.

	stream := &testSendSnapshotServer{
		chunks: []*pb.EtcdRaftSnapshotChunk{
			{Metadata: raw, Chunk: payload, Final: true},
		},
	}
	msg, err := transport.receiveSnapshotStream(stream)
	require.NoError(t, err)
	require.NotNil(t, msg.Snapshot)
	require.Equal(t, payload, msg.Snapshot.Data)
	require.False(t, isSnapshotToken(msg.Snapshot.Data))
}

func TestClientForDeduplicatesConcurrentDial(t *testing.T) {
	transport := NewGRPCTransport([]Peer{{
		NodeID:  2,
		ID:      "n2",
		Address: "127.0.0.1:65530",
	}})
	t.Cleanup(func() {
		require.NoError(t, transport.Close())
	})

	oldNewClient := grpcNewClient
	var callCount atomic.Int32
	grpcNewClient = func(target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
		callCount.Add(1)
		return oldNewClient(target, opts...)
	}
	t.Cleanup(func() {
		grpcNewClient = oldNewClient
	})

	const callers = 8
	var wg sync.WaitGroup
	wg.Add(callers)
	clients := make([]pb.EtcdRaftClient, callers)
	errCh := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func(idx int) {
			defer wg.Done()
			client, err := transport.clientFor(2)
			if err != nil {
				errCh <- err
				return
			}
			clients[idx] = client
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
	require.Equal(t, int32(1), callCount.Load())
	for i := 1; i < callers; i++ {
		require.Equal(t, clients[0], clients[i])
	}
}

func TestNewGRPCTransportDerivesPeerNodeIDs(t *testing.T) {
	transport := NewGRPCTransport([]Peer{{
		ID:      "n2",
		Address: "127.0.0.1:65530",
	}})

	peer, err := transport.peerFor(DeriveNodeID("n2"))
	require.NoError(t, err)
	require.Equal(t, "n2", peer.ID)
	require.Equal(t, "127.0.0.1:65530", peer.Address)
}

func TestSendStreamReceivesRegularMessages(t *testing.T) {
	reqs := []*pb.EtcdRaftMessage{
		mustEtcdRaftMessage(t, raftpb.Message{Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(1), To: uint64Ptr(2), Term: uint64Ptr(3), Index: uint64Ptr(10)}),
		mustEtcdRaftMessage(t, raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat), From: uint64Ptr(1), To: uint64Ptr(2), Term: uint64Ptr(3), Commit: uint64Ptr(9)}),
	}
	transport := NewGRPCTransport(nil)
	var got []raftpb.Message
	transport.SetHandler(func(_ context.Context, msg raftpb.Message) error {
		got = append(got, msg)
		return nil
	})
	stream := &testSendStreamServer{messages: reqs}

	require.NoError(t, transport.SendStream(stream))
	require.True(t, stream.closed)
	require.Len(t, got, 2)
	require.Equal(t, raftpb.MsgApp, got[0].GetType())
	require.Equal(t, uint64(10), got[0].GetIndex())
	require.Equal(t, raftpb.MsgHeartbeat, got[1].GetType())
	require.Equal(t, uint64(9), got[1].GetCommit())
}

func TestDispatchRegularUsesSendStream(t *testing.T) {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	recvTransport := NewGRPCTransport(nil)
	gotCh := make(chan raftpb.Message, 2)
	recvTransport.SetHandler(func(_ context.Context, msg raftpb.Message) error {
		gotCh <- msg
		return nil
	})

	server := grpc.NewServer()
	recvTransport.Register(server)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(lis) }()

	sendTransport := NewGRPCTransport([]Peer{{NodeID: 2, Address: lis.Addr().String()}})
	t.Cleanup(func() { require.NoError(t, sendTransport.Close()) })

	require.NoError(t, sendTransport.dispatchRegular(context.Background(), raftpb.Message{
		Type:  messageTypePtr(raftpb.MsgApp),
		From:  uint64Ptr(1),
		To:    uint64Ptr(2),
		Term:  uint64Ptr(4),
		Index: uint64Ptr(22),
	}))
	require.NoError(t, sendTransport.dispatchRegular(context.Background(), raftpb.Message{
		Type:   messageTypePtr(raftpb.MsgHeartbeat),
		From:   uint64Ptr(1),
		To:     uint64Ptr(2),
		Term:   uint64Ptr(4),
		Commit: uint64Ptr(22),
	}))

	got := collectRaftMessages(t, gotCh, 2)
	var gotApp, gotHeartbeat bool
	for _, msg := range got {
		switch msg.GetType() { //nolint:exhaustive // this test expects only the two messages dispatched above.
		case raftpb.MsgApp:
			gotApp = true
			require.Equal(t, uint64(22), msg.GetIndex())
		case raftpb.MsgHeartbeat:
			gotHeartbeat = true
			require.Equal(t, uint64(22), msg.GetCommit())
		default:
			t.Fatalf("unexpected raft message type %s", msg.GetType())
		}
	}
	require.True(t, gotApp)
	require.True(t, gotHeartbeat)

	sendTransport.mu.RLock()
	_, streamCached := sendTransport.streams[lis.Addr().String()]
	streamSupported := sendTransport.streamSupported[lis.Addr().String()]
	sendTransport.mu.RUnlock()
	require.True(t, streamCached)
	require.True(t, streamSupported)
}

func TestDispatchRegularFallsBackToUnaryWhenSendStreamUnimplemented(t *testing.T) {
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	legacy := &legacyEtcdRaftServer{got: make(chan raftpb.Message, 2)}
	server := grpc.NewServer()
	pb.RegisterEtcdRaftServer(server, legacy)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(lis) }()

	sendTransport := NewGRPCTransport([]Peer{{NodeID: 2, Address: lis.Addr().String()}})
	t.Cleanup(func() { require.NoError(t, sendTransport.Close()) })

	require.NoError(t, sendTransport.dispatchRegular(context.Background(), raftpb.Message{
		Type:  messageTypePtr(raftpb.MsgApp),
		From:  uint64Ptr(1),
		To:    uint64Ptr(2),
		Term:  uint64Ptr(5),
		Index: uint64Ptr(30),
	}))
	require.True(t, sendTransport.peerStreamUnsupported(lis.Addr().String()))

	require.NoError(t, sendTransport.dispatchRegular(context.Background(), raftpb.Message{
		Type:  messageTypePtr(raftpb.MsgAppResp),
		From:  uint64Ptr(1),
		To:    uint64Ptr(2),
		Term:  uint64Ptr(5),
		Index: uint64Ptr(31),
	}))

	got := collectRaftMessages(t, legacy.got, 2)
	require.Equal(t, raftpb.MsgApp, got[0].GetType())
	require.Equal(t, uint64(30), got[0].GetIndex())
	require.Equal(t, raftpb.MsgAppResp, got[1].GetType())
	require.Equal(t, uint64(31), got[1].GetIndex())
}

func TestDispatchRegularUsesUnaryForPriorityMessages(t *testing.T) {
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	client := &testEtcdRaftClient{}
	injectClient(t, transport, addr, client)

	require.NoError(t, transport.dispatchRegular(context.Background(), raftpb.Message{
		Type:   messageTypePtr(raftpb.MsgHeartbeat),
		From:   uint64Ptr(1),
		To:     uint64Ptr(2),
		Term:   uint64Ptr(4),
		Commit: uint64Ptr(22),
	}))

	require.Equal(t, int32(1), client.sendCalls.Load())
	require.Zero(t, client.sendStreamCalls.Load())
}

func TestDispatchRegularDropsCachedPeerConnAfterUnaryUnavailable(t *testing.T) {
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	client := &testEtcdRaftClient{sendErr: status.Error(codes.Unavailable, "connection refused")}
	injectClient(t, transport, addr, client)

	err := transport.dispatchRegular(context.Background(), raftpb.Message{
		Type:   messageTypePtr(raftpb.MsgHeartbeat),
		From:   uint64Ptr(1),
		To:     uint64Ptr(2),
		Term:   uint64Ptr(4),
		Commit: uint64Ptr(22),
	})
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, grpcStatusCode(err))

	transport.mu.RLock()
	_, cached := transport.clients[addr]
	transport.mu.RUnlock()
	require.False(t, cached)
	require.Equal(t, int32(1), client.sendCalls.Load())
}

func TestDispatchRegularUsesUnaryWhenSendStreamDisabled(t *testing.T) {
	t.Setenv(sendStreamEnabledEnvVar, "false")
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	client := &testEtcdRaftClient{}
	injectClient(t, transport, addr, client)

	require.NoError(t, transport.dispatchRegular(context.Background(), raftpb.Message{
		Type:  messageTypePtr(raftpb.MsgApp),
		From:  uint64Ptr(1),
		To:    uint64Ptr(2),
		Term:  uint64Ptr(4),
		Index: uint64Ptr(22),
	}))

	require.Equal(t, int32(1), client.sendCalls.Load())
	require.Zero(t, client.sendStreamCalls.Load())
	transport.mu.RLock()
	_, streamCached := transport.streams[addr]
	transport.mu.RUnlock()
	require.False(t, streamCached)
}

func TestSetSendStreamEnabledClosesCachedStreams(t *testing.T) {
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	client := &testEtcdRaftClient{}
	injectClient(t, transport, addr, client)

	require.NoError(t, transport.dispatchRegular(context.Background(), raftpb.Message{
		Type:  messageTypePtr(raftpb.MsgApp),
		From:  uint64Ptr(1),
		To:    uint64Ptr(2),
		Term:  uint64Ptr(4),
		Index: uint64Ptr(22),
	}))
	transport.mu.RLock()
	_, streamCached := transport.streams[addr]
	transport.mu.RUnlock()
	require.True(t, streamCached)

	transport.SetSendStreamEnabled(false)

	transport.mu.RLock()
	_, streamCached = transport.streams[addr]
	transport.mu.RUnlock()
	require.False(t, streamCached)
}

func TestStreamForAbortsCachingWhenSendStreamDisabledDuringOpen(t *testing.T) {
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })

	openStarted := make(chan struct{})
	client := &testEtcdRaftClient{
		raftStreams:                 []*testRaftSendStreamClient{{}},
		blockSendStreamCall:         2,
		blockSendStreamUntilContext: true,
		sendStreamStarted:           openStarted,
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := transport.streamFor(context.Background(), addr, client)
		errCh <- err
	}()

	select {
	case <-openStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for SendStream open")
	}
	transport.SetSendStreamEnabled(false)

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.Equal(t, codes.Unavailable, grpcStatusCode(err))
		require.True(t, isSendStreamDisabled(err))
	case <-time.After(5 * time.Second):
		t.Fatal("streamFor did not return after SendStream was disabled")
	}

	transport.mu.RLock()
	_, streamCached := transport.streams[addr]
	transport.mu.RUnlock()
	require.False(t, streamCached)
}

func TestDispatchRegularFallsBackToUnaryWhenSendStreamDisabledDuringSend(t *testing.T) {
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	sendStarted := make(chan struct{})
	client := &testEtcdRaftClient{
		raftStreams: []*testRaftSendStreamClient{
			{},
			{blockSend: true, sendStarted: sendStarted},
		},
	}
	injectClient(t, transport, addr, client)

	errCh := make(chan error, 1)
	go func() {
		errCh <- transport.dispatchRegular(context.Background(), raftpb.Message{
			Type:  messageTypePtr(raftpb.MsgApp),
			From:  uint64Ptr(1),
			To:    uint64Ptr(2),
			Term:  uint64Ptr(5),
			Index: uint64Ptr(50),
		})
	}()

	select {
	case <-sendStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream Send to start")
	}
	transport.SetSendStreamEnabled(false)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("dispatchRegular did not fall back after SendStream was disabled")
	}

	require.Equal(t, int32(1), client.sendCalls.Load())
	transport.mu.RLock()
	_, streamCached := transport.streams[addr]
	transport.mu.RUnlock()
	require.False(t, streamCached)
}

func TestDispatchRegularFallsBackToUnaryWhenSendStreamDisabledDuringProbe(t *testing.T) {
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	probeStarted := make(chan struct{})
	client := &testEtcdRaftClient{
		blockSendStreamCall:         1,
		blockSendStreamUntilContext: true,
		sendStreamStarted:           probeStarted,
	}
	injectClient(t, transport, addr, client)

	errCh := make(chan error, 1)
	go func() {
		errCh <- transport.dispatchRegular(context.Background(), raftpb.Message{
			Type:  messageTypePtr(raftpb.MsgApp),
			From:  uint64Ptr(1),
			To:    uint64Ptr(2),
			Term:  uint64Ptr(5),
			Index: uint64Ptr(51),
		})
	}()

	select {
	case <-probeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for SendStream probe")
	}
	transport.SetSendStreamEnabled(false)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("dispatchRegular did not fall back after SendStream probe was disabled")
	}

	require.Equal(t, int32(1), client.sendCalls.Load())
	require.Equal(t, int32(1), client.sendStreamCalls.Load())
}

func TestDispatchRegularFallsBackToUnaryWhenSendStreamDisabledDuringOpen(t *testing.T) {
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	openStarted := make(chan struct{})
	client := &testEtcdRaftClient{
		raftStreams:                 []*testRaftSendStreamClient{{}},
		blockSendStreamCall:         2,
		blockSendStreamUntilContext: true,
		sendStreamStarted:           openStarted,
	}
	injectClient(t, transport, addr, client)

	errCh := make(chan error, 1)
	go func() {
		errCh <- transport.dispatchRegular(context.Background(), raftpb.Message{
			Type:  messageTypePtr(raftpb.MsgApp),
			From:  uint64Ptr(1),
			To:    uint64Ptr(2),
			Term:  uint64Ptr(5),
			Index: uint64Ptr(52),
		})
	}()

	select {
	case <-openStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for SendStream open")
	}
	transport.SetSendStreamEnabled(false)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("dispatchRegular did not fall back after SendStream open was disabled")
	}

	require.Equal(t, int32(1), client.sendCalls.Load())
	require.Equal(t, int32(2), client.sendStreamCalls.Load())
	transport.mu.RLock()
	_, streamCached := transport.streams[addr]
	transport.mu.RUnlock()
	require.False(t, streamCached)
}

func TestDispatchRegularFallsBackToUnaryForSendStreamDisabledErrorAfterReenable(t *testing.T) {
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	client := &testEtcdRaftClient{
		raftStreams: []*testRaftSendStreamClient{
			{},
			{sendErr: sendStreamDisabledError()},
		},
	}
	injectClient(t, transport, addr, client)

	require.NoError(t, transport.dispatchRegular(context.Background(), raftpb.Message{
		Type:  messageTypePtr(raftpb.MsgApp),
		From:  uint64Ptr(1),
		To:    uint64Ptr(2),
		Term:  uint64Ptr(5),
		Index: uint64Ptr(53),
	}))

	require.True(t, transport.sendStreamEnabledNow())
	require.Equal(t, int32(1), client.sendCalls.Load())
	require.Equal(t, int32(2), client.sendStreamCalls.Load())
}

func TestDispatchRegularReprobesStreamAfterUnsupportedCacheExpires(t *testing.T) {
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	client := &testEtcdRaftClient{
		raftStreams: []*testRaftSendStreamClient{{}, {}},
	}
	injectClient(t, transport, addr, client)

	transport.markPeerStreamUnsupported(addr)
	require.NoError(t, transport.dispatchRegular(context.Background(), raftpb.Message{
		Type:  messageTypePtr(raftpb.MsgApp),
		From:  uint64Ptr(1),
		To:    uint64Ptr(2),
		Term:  uint64Ptr(5),
		Index: uint64Ptr(40),
	}))
	require.Equal(t, int32(1), client.sendCalls.Load())
	require.Zero(t, client.sendStreamCalls.Load())

	transport.mu.Lock()
	transport.streamUnsupportedAt[addr] = time.Now().Add(-defaultSendStreamReprobeInterval - time.Second)
	transport.mu.Unlock()

	require.NoError(t, transport.dispatchRegular(context.Background(), raftpb.Message{
		Type:  messageTypePtr(raftpb.MsgApp),
		From:  uint64Ptr(1),
		To:    uint64Ptr(2),
		Term:  uint64Ptr(5),
		Index: uint64Ptr(41),
	}))
	require.Equal(t, int32(1), client.sendCalls.Load())
	require.Equal(t, int32(2), client.sendStreamCalls.Load())
	require.False(t, transport.peerStreamUnsupported(addr))

	transport.mu.RLock()
	streamSupported := transport.streamSupported[addr]
	_, streamCached := transport.streams[addr]
	transport.mu.RUnlock()
	require.True(t, streamSupported)
	require.True(t, streamCached)
}

func TestStreamForDeduplicatesConcurrentOpen(t *testing.T) {
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	client := &testEtcdRaftClient{}

	const callers = 8
	var wg sync.WaitGroup
	wg.Add(callers)
	streams := make([]*peerStream, callers)
	errCh := make(chan error, callers)
	for i := 0; i < callers; i++ {
		go func(idx int) {
			defer wg.Done()
			stream, err := transport.streamFor(context.Background(), addr, client)
			if err != nil {
				errCh <- err
				return
			}
			streams[idx] = stream
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
	require.Equal(t, int32(2), client.sendStreamCalls.Load(), "one probe stream plus one cached send stream")
	for i := 1; i < callers; i++ {
		require.Equal(t, streams[0], streams[i])
	}
}

func TestDispatchRegularStreamCancelsCachedStreamWhenContextDone(t *testing.T) {
	const addr = "host:2"
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: addr}})
	t.Cleanup(func() { require.NoError(t, transport.Close()) })
	sendStarted := make(chan struct{})
	client := &testEtcdRaftClient{
		raftStreams: []*testRaftSendStreamClient{
			{},
			{blockSend: true, sendStarted: sendStarted},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- transport.dispatchRegularStream(ctx, addr, client, mustEtcdRaftMessage(t, raftpb.Message{
			Type:  messageTypePtr(raftpb.MsgApp),
			From:  uint64Ptr(1),
			To:    uint64Ptr(2),
			Term:  uint64Ptr(5),
			Index: uint64Ptr(42),
		}))
	}()

	select {
	case <-sendStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream Send to start")
	}
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("dispatchRegularStream did not return after context cancellation")
	}

	transport.mu.RLock()
	_, streamCached := transport.streams[addr]
	transport.mu.RUnlock()
	require.False(t, streamCached)
}

type testSendSnapshotServer struct {
	chunks []*pb.EtcdRaftSnapshotChunk
	index  int
}

func (s *testSendSnapshotServer) Recv() (*pb.EtcdRaftSnapshotChunk, error) {
	if s.index >= len(s.chunks) {
		return nil, io.EOF
	}
	chunk := s.chunks[s.index]
	s.index++
	return chunk, nil
}

func (*testSendSnapshotServer) SendAndClose(*pb.EtcdRaftAck) error {
	return nil
}

func (*testSendSnapshotServer) SetHeader(metadata.MD) error {
	return nil
}

func (*testSendSnapshotServer) SendHeader(metadata.MD) error {
	return nil
}

func (*testSendSnapshotServer) SetTrailer(metadata.MD) {}

func (*testSendSnapshotServer) Context() context.Context {
	return context.Background()
}

func (*testSendSnapshotServer) SendMsg(any) error {
	return nil
}

func (*testSendSnapshotServer) RecvMsg(any) error {
	return nil
}

type testSendStreamServer struct {
	messages []*pb.EtcdRaftMessage
	index    int
	closed   bool
}

func (s *testSendStreamServer) Recv() (*pb.EtcdRaftMessage, error) {
	if s.index >= len(s.messages) {
		return nil, io.EOF
	}
	msg := s.messages[s.index]
	s.index++
	return msg, nil
}

func (s *testSendStreamServer) SendAndClose(*pb.EtcdRaftAck) error {
	s.closed = true
	return nil
}

func (*testSendStreamServer) SetHeader(metadata.MD) error {
	return nil
}

func (*testSendStreamServer) SendHeader(metadata.MD) error {
	return nil
}

func (*testSendStreamServer) SetTrailer(metadata.MD) {}

func (*testSendStreamServer) Context() context.Context {
	return context.Background()
}

func (*testSendStreamServer) SendMsg(any) error {
	return nil
}

func (*testSendStreamServer) RecvMsg(any) error {
	return nil
}

type legacyEtcdRaftServer struct {
	pb.UnimplementedEtcdRaftServer
	got chan raftpb.Message
}

func (s *legacyEtcdRaftServer) Send(_ context.Context, req *pb.EtcdRaftMessage) (*pb.EtcdRaftAck, error) {
	var msg raftpb.Message
	if err := proto.Unmarshal(req.GetMessage(), &msg); err != nil {
		return nil, err
	}
	s.got <- msg
	return &pb.EtcdRaftAck{}, nil
}

func mustEtcdRaftMessage(t *testing.T, msg raftpb.Message) *pb.EtcdRaftMessage {
	t.Helper()
	raw, err := proto.Marshal(&msg)
	require.NoError(t, err)
	return &pb.EtcdRaftMessage{Message: raw}
}

func collectRaftMessages(t *testing.T, ch <-chan raftpb.Message, count int) []raftpb.Message {
	t.Helper()
	got := make([]raftpb.Message, 0, count)
	for len(got) < count {
		select {
		case msg := <-ch:
			got = append(got, msg)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for raft message %d/%d", len(got)+1, count)
		}
	}
	return got
}

// --- applyBridgeMode tests ---

func TestApplyBridgeModePassesNonTokenUnchanged(t *testing.T) {
	transport := NewGRPCTransport(nil)

	// Non-token payload must pass through unchanged.
	msg := raftpb.Message{
		Snapshot: &raftpb.Snapshot{Data: []byte("legacy full payload")},
	}
	patched, err := transport.applyBridgeMode(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, []byte("legacy full payload"), patched.Snapshot.Data)
}

func TestApplyBridgeModeNoReaderIsNoop(t *testing.T) {
	transport := NewGRPCTransport(nil)

	// A token with no readFSMPayload callback set → passthrough.
	token := encodeSnapshotToken(42, 0xDEADBEEF)
	msg := raftpb.Message{
		Snapshot: &raftpb.Snapshot{Data: token},
	}
	patched, err := transport.applyBridgeMode(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, token, patched.Snapshot.Data)
}

func TestApplyBridgeModeReconstructsPayload(t *testing.T) {
	fsmSnapDir := t.TempDir()
	payload := []byte("bridge mode payload data 12345")
	crc, _ := writeFSMFileForTest(t, fsmSnapDir, 42, payload)

	transport := NewGRPCTransport(nil)
	transport.SetFSMPayloadReader(func(index uint64) ([]byte, error) {
		return readFSMSnapshotPayload(fsmSnapPath(fsmSnapDir, index))
	})

	token := encodeSnapshotToken(42, crc)
	msg := raftpb.Message{
		Snapshot: &raftpb.Snapshot{
			Data:     token,
			Metadata: testSnapshotMetadata(42, 0, nil),
		},
	}
	patched, err := transport.applyBridgeMode(context.Background(), msg)
	require.NoError(t, err)
	require.Equal(t, payload, patched.Snapshot.Data)
	// Metadata must be preserved.
	require.Equal(t, uint64(42), patched.Snapshot.GetMetadata().GetIndex())
}

func TestApplyBridgeModeReaderError(t *testing.T) {
	transport := NewGRPCTransport(nil)
	transport.SetFSMPayloadReader(func(_ uint64) ([]byte, error) {
		return nil, ErrFSMSnapshotNotFound
	})

	token := encodeSnapshotToken(99, 0xABCD)
	msg := raftpb.Message{
		Snapshot: &raftpb.Snapshot{Data: token},
	}
	_, err := transport.applyBridgeMode(context.Background(), msg)
	require.ErrorIs(t, err, ErrFSMSnapshotNotFound)
}

// TestSendSnapshotReaderChunksSmallPayloadPreservesData is a regression test
// for a bug where a payload smaller than one chunk size was silently dropped.
// readSnapshotChunk returns (data, io.EOF) for a short final read, and the old
// code treated that the same as an empty reader, sending an empty chunk.
func TestSendSnapshotReaderChunksSmallPayloadPreservesData(t *testing.T) {
	payload := []byte("tiny payload under one chunk")
	header := []byte("header-bytes")

	client := &testSnapshotSendClient{}
	err := sendSnapshotReaderChunks(client, header, bytes.NewReader(payload), defaultSnapshotChunkSize)
	require.NoError(t, err)

	require.Len(t, client.chunks, 1)
	require.Equal(t, header, client.chunks[0].Metadata)
	require.Equal(t, payload, client.chunks[0].Chunk)
	require.True(t, client.chunks[0].Final)
}

func TestSendSnapshotReaderChunksEmptyPayloadSendsHeaderOnly(t *testing.T) {
	header := []byte("header-bytes")

	client := &testSnapshotSendClient{}
	err := sendSnapshotReaderChunks(client, header, bytes.NewReader(nil), defaultSnapshotChunkSize)
	require.NoError(t, err)

	require.Len(t, client.chunks, 1)
	require.Equal(t, header, client.chunks[0].Metadata)
	require.Empty(t, client.chunks[0].Chunk)
	require.True(t, client.chunks[0].Final)
}

// testSnapshotSendClient captures chunks sent via sendSnapshotReaderChunks / sendSnapshotChunks.
type testSnapshotSendClient struct {
	chunks []*pb.EtcdRaftSnapshotChunk
}

func (c *testSnapshotSendClient) Send(chunk *pb.EtcdRaftSnapshotChunk) error {
	c.chunks = append(c.chunks, chunk)
	return nil
}

func (c *testSnapshotSendClient) CloseAndRecv() (*pb.EtcdRaftAck, error) {
	return &pb.EtcdRaftAck{}, nil
}

func (*testSnapshotSendClient) Header() (metadata.MD, error) { return nil, nil }
func (*testSnapshotSendClient) Trailer() metadata.MD         { return nil }
func (*testSnapshotSendClient) CloseSend() error             { return nil }
func (*testSnapshotSendClient) Context() context.Context     { return context.Background() }
func (*testSnapshotSendClient) SendMsg(any) error            { return nil }
func (*testSnapshotSendClient) RecvMsg(any) error            { return nil }

// testEtcdRaftClient is a minimal mock of pb.EtcdRaftClient that routes
// SendSnapshot calls to a pre-wired testSnapshotSendClient.
type testEtcdRaftClient struct {
	stream                      *testSnapshotSendClient
	raftStream                  *testRaftSendStreamClient
	raftStreams                 []*testRaftSendStreamClient
	blockSendStreamCall         int32
	blockSendStreamUntilContext bool
	sendStreamStarted           chan struct{}
	releaseSendStream           chan struct{}
	sendErr                     error
	sendCalls                   atomic.Int32
	sendStreamCalls             atomic.Int32
}

func (c *testEtcdRaftClient) Send(_ context.Context, _ *pb.EtcdRaftMessage, _ ...grpc.CallOption) (*pb.EtcdRaftAck, error) {
	c.sendCalls.Add(1)
	if c.sendErr != nil {
		return nil, c.sendErr
	}
	return &pb.EtcdRaftAck{}, nil
}

func (c *testEtcdRaftClient) SendStream(ctx context.Context, _ ...grpc.CallOption) (pb.EtcdRaft_SendStreamClient, error) {
	call := c.sendStreamCalls.Add(1)
	if c.blockSendStreamCall != 0 && call == c.blockSendStreamCall {
		if c.sendStreamStarted != nil {
			close(c.sendStreamStarted)
		}
		if c.blockSendStreamUntilContext {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if c.releaseSendStream != nil {
			<-c.releaseSendStream
		}
	}
	if len(c.raftStreams) > 0 {
		stream := c.raftStreams[0]
		c.raftStreams = c.raftStreams[1:]
		stream.ctx = ctx
		return stream, nil
	}
	if c.raftStream != nil {
		c.raftStream.ctx = ctx
		return c.raftStream, nil
	}
	return &testRaftSendStreamClient{ctx: ctx}, nil
}

func (c *testEtcdRaftClient) SendSnapshot(_ context.Context, _ ...grpc.CallOption) (pb.EtcdRaft_SendSnapshotClient, error) {
	return c.stream, nil
}

type testRaftSendStreamClient struct {
	messages    []*pb.EtcdRaftMessage
	closed      bool
	ctx         context.Context
	recvErr     chan error
	sendErr     error
	blockSend   bool
	sendStarted chan struct{}
	sendOnce    sync.Once
}

func (c *testRaftSendStreamClient) Send(msg *pb.EtcdRaftMessage) error {
	c.messages = append(c.messages, msg)
	if c.sendStarted != nil {
		c.sendOnce.Do(func() { close(c.sendStarted) })
	}
	if c.blockSend {
		<-c.Context().Done()
		return c.Context().Err()
	}
	if c.sendErr != nil {
		return c.sendErr
	}
	return nil
}

func (c *testRaftSendStreamClient) CloseAndRecv() (*pb.EtcdRaftAck, error) {
	c.closed = true
	return &pb.EtcdRaftAck{}, nil
}

func (*testRaftSendStreamClient) Header() (metadata.MD, error) { return nil, nil }
func (*testRaftSendStreamClient) Trailer() metadata.MD         { return nil }
func (*testRaftSendStreamClient) CloseSend() error             { return nil }
func (c *testRaftSendStreamClient) Context() context.Context {
	if c.ctx != nil {
		return c.ctx
	}
	return context.Background()
}
func (*testRaftSendStreamClient) SendMsg(any) error { return nil }
func (c *testRaftSendStreamClient) RecvMsg(any) error {
	if c.recvErr != nil {
		select {
		case err := <-c.recvErr:
			return err
		case <-c.Context().Done():
			return c.Context().Err()
		}
	}
	<-c.Context().Done()
	return c.Context().Err()
}

// injectClient pre-populates the transport's client cache for the given peer
// address so calls to clientFor return the mock without dialling.
func injectClient(t *testing.T, transport *GRPCTransport, address string, client pb.EtcdRaftClient) {
	t.Helper()
	transport.mu.Lock()
	transport.clients[address] = client
	transport.mu.Unlock()
}

// --- sendSnapshotReaderChunks multi-chunk tests ---

func TestSendSnapshotReaderChunksMultiChunk(t *testing.T) {
	// 12-byte payload with chunkSize=4 → 3 chunks.
	chunkSize := 4
	payload := []byte("1234abcd5678")
	header := []byte("meta")

	client := &testSnapshotSendClient{}
	err := sendSnapshotReaderChunks(client, header, bytes.NewReader(payload), chunkSize)
	require.NoError(t, err)

	require.Len(t, client.chunks, 3)

	require.Equal(t, header, client.chunks[0].Metadata)
	require.Equal(t, []byte("1234"), client.chunks[0].Chunk)
	require.False(t, client.chunks[0].Final)

	require.Empty(t, client.chunks[1].Metadata)
	require.Equal(t, []byte("abcd"), client.chunks[1].Chunk)
	require.False(t, client.chunks[1].Final)

	require.Empty(t, client.chunks[2].Metadata)
	require.Equal(t, []byte("5678"), client.chunks[2].Chunk)
	require.True(t, client.chunks[2].Final)
}

func TestSendSnapshotReaderChunksExactBoundary(t *testing.T) {
	// 8-byte payload with chunkSize=4 → exactly 2 chunks.
	chunkSize := 4
	payload := []byte("12345678")
	header := []byte("hdr")

	client := &testSnapshotSendClient{}
	err := sendSnapshotReaderChunks(client, header, bytes.NewReader(payload), chunkSize)
	require.NoError(t, err)

	require.Len(t, client.chunks, 2)
	require.Equal(t, header, client.chunks[0].Metadata)
	require.Equal(t, []byte("1234"), client.chunks[0].Chunk)
	require.False(t, client.chunks[0].Final)

	require.Equal(t, []byte("5678"), client.chunks[1].Chunk)
	require.True(t, client.chunks[1].Final)
}

// TestSendSnapshotReaderChunksTrailingPartialChunk regressions a production
// failure where payloads whose length was not a whole multiple of chunkSize
// had the trailing partial chunk silently dropped. The old loop emitted the
// last full chunk with Final=true and returned, leaving the partial in
// `next` unsent. Receivers observed payload_bytes truncated to the previous
// boundary and FSM.Restore hit readRestoreEntry with unexpected EOF.
//
// In the reproduction the exactly-on-boundary case already passed, so the
// pre-existing TestSendSnapshotReaderChunksExactBoundary missed the bug.
// This test fixes the coverage gap: chunkSize=4 with a 9-byte payload yields
// two full chunks plus a 1-byte tail that must arrive.
func TestSendSnapshotReaderChunksTrailingPartialChunk(t *testing.T) {
	chunkSize := 4
	payload := []byte("12345678X")
	header := []byte("hdr")

	client := &testSnapshotSendClient{}
	err := sendSnapshotReaderChunks(client, header, bytes.NewReader(payload), chunkSize)
	require.NoError(t, err)

	require.Len(t, client.chunks, 3, "expected two full chunks plus a trailing partial")

	require.Equal(t, header, client.chunks[0].Metadata)
	require.Equal(t, []byte("1234"), client.chunks[0].Chunk)
	require.False(t, client.chunks[0].Final)

	require.Equal(t, []byte("5678"), client.chunks[1].Chunk)
	require.False(t, client.chunks[1].Final)

	require.Equal(t, []byte("X"), client.chunks[2].Chunk)
	require.True(t, client.chunks[2].Final)

	var delivered []byte
	for _, c := range client.chunks {
		delivered = append(delivered, c.Chunk...)
	}
	require.Equal(t, payload, delivered)
}

// --- streamFSMSnapshot tests ---

// TestStreamFSMSnapshotOverGRPCRestoresFollowerFSM exercises the full
// sender-to-receiver streaming path over a real gRPC server and then drives
// the received bytes through StateMachine.Restore on a fresh follower FSM,
// simulating the deployment scenario the bug manifested in (a follower whose
// data directory was wiped and then received a snapshot from the leader).
//
// The reproducer uses a 3-entry testStateMachine serialization whose total
// byte size (31 bytes) is not a whole multiple of the forced chunkSize (8).
// Before the trailing-partial fix in sendSnapshotReaderChunks the receiver
// observed the stream truncated to 24 bytes (3 full chunks) and the
// subsequent Restore failed with io.ErrUnexpectedEOF while reading the last
// length-prefixed item. The assertion chain is therefore:
//
//  1. sender streams the full .fsm file contents,
//  2. receiver accumulates all chunks and reconstructs the message,
//  3. a *fresh* receiver FSM restores from msg.Snapshot.Data and exposes
//     exactly the entries that were serialized on the sender side.
//
// (3) is the real acceptance criterion — (1) and (2) are stepping stones.
func TestStreamFSMSnapshotOverGRPCRestoresFollowerFSM(t *testing.T) {
	// Seed a sender-side state machine with known entries and serialize it
	// through its own Snapshot() codec so the payload format matches what
	// Restore expects.
	senderFSM := &testStateMachine{}
	for _, e := range [][]byte{[]byte("alpha"), []byte("bravo"), []byte("charlie")} {
		senderFSM.Apply(e)
	}
	snap, err := senderFSM.Snapshot()
	require.NoError(t, err)
	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)
	require.NoError(t, snap.Close())
	payload := buf.Bytes()

	dir := t.TempDir()
	crc, _ := writeFSMFileForTest(t, dir, 77, payload)

	// Pick a chunk size that guarantees a trailing partial — without this
	// shape the pre-fix code path would have exited normally and the test
	// would be useless as a regression guard.
	const chunkSize = 8
	require.NotZero(t, len(payload)%chunkSize,
		"payload length %d must not be a whole multiple of chunkSize %d or this test would no longer cover the trailing-partial bug",
		len(payload), chunkSize)

	// Real gRPC server with receiver transport.
	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	recvTransport := NewGRPCTransport(nil)
	recvTransport.snapshotChunkSize = chunkSize
	recvTransport.SetSpoolDir(t.TempDir())

	receiverFSM := &testStateMachine{}
	restoredCh := make(chan error, 1)
	recvTransport.SetHandler(func(_ context.Context, msg raftpb.Message) error {
		if msg.Snapshot == nil {
			restoredCh <- errors.New("nil snapshot on receiver")
			return nil
		}
		restoredCh <- receiverFSM.Restore(bytes.NewReader(msg.Snapshot.Data))
		return nil
	})

	server := grpc.NewServer()
	recvTransport.Register(server)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(lis) }()

	sendTransport := NewGRPCTransport([]Peer{{NodeID: 2, Address: lis.Addr().String()}})
	sendTransport.snapshotChunkSize = chunkSize
	t.Cleanup(func() { require.NoError(t, sendTransport.Close()) })

	openFn := func(index uint64) (io.ReadCloser, error) {
		return openFSMSnapshotPayloadReader(fsmSnapPath(dir, index))
	}
	msg := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap), From: uint64Ptr(1), To: uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Data:     encodeSnapshotToken(77, crc),
			Metadata: testSnapshotMetadata(77, 5, nil),
		},
	}
	require.NoError(t, sendTransport.streamFSMSnapshot(context.Background(), msg, 77, openFn))

	select {
	case restoreErr := <-restoredCh:
		require.NoError(t, restoreErr,
			"fresh follower FSM must restore cleanly; a short-read EOF here means the trailing partial chunk was dropped in transit")
	case <-time.After(5 * time.Second):
		t.Fatal("receiver never ran Restore")
	}

	require.Equal(t, senderFSM.Applied(), receiverFSM.Applied(),
		"receiver FSM state after Restore must equal sender FSM state")
}

// TestStreamFSMSnapshotOverGRPCAtChunkBoundary pins the exact-multiple case
// so a future refactor cannot regress it while fixing the non-aligned case.
func TestStreamFSMSnapshotOverGRPCAtChunkBoundary(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("AAAAAAAABBBBBBBB") // 16 bytes == 2 × chunkSize(8)
	crc, _ := writeFSMFileForTest(t, dir, 91, payload)

	lis, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = lis.Close() })

	recvTransport := NewGRPCTransport(nil)
	recvTransport.snapshotChunkSize = 8
	recvTransport.SetSpoolDir(t.TempDir())
	gotCh := make(chan raftpb.Message, 1)
	recvTransport.SetHandler(func(_ context.Context, msg raftpb.Message) error {
		gotCh <- msg
		return nil
	})

	server := grpc.NewServer()
	recvTransport.Register(server)
	t.Cleanup(server.Stop)
	go func() { _ = server.Serve(lis) }()

	sendTransport := NewGRPCTransport([]Peer{{NodeID: 2, Address: lis.Addr().String()}})
	sendTransport.snapshotChunkSize = 8
	t.Cleanup(func() { require.NoError(t, sendTransport.Close()) })

	openFn := func(index uint64) (io.ReadCloser, error) {
		return openFSMSnapshotPayloadReader(fsmSnapPath(dir, index))
	}
	msg := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap), From: uint64Ptr(1), To: uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Data:     encodeSnapshotToken(91, crc),
			Metadata: testSnapshotMetadata(91, 5, nil),
		},
	}
	require.NoError(t, sendTransport.streamFSMSnapshot(context.Background(), msg, 91, openFn))

	select {
	case got := <-gotCh:
		require.Equal(t, payload, got.Snapshot.Data)
	case <-time.After(5 * time.Second):
		t.Fatal("receiver never observed the snapshot message")
	}
}

func TestStreamFSMSnapshotSendsPayload(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("stream fsm snapshot payload data for test")
	crc, _ := writeFSMFileForTest(t, dir, 55, payload)

	sendClient := &testSnapshotSendClient{}
	transport := NewGRPCTransport([]Peer{{NodeID: 3, Address: "host:2"}})
	injectClient(t, transport, "host:2", &testEtcdRaftClient{stream: sendClient})

	openFn := func(index uint64) (io.ReadCloser, error) {
		return openFSMSnapshotPayloadReader(fsmSnapPath(dir, index))
	}

	msg := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		To:   uint64Ptr(3),
		Snapshot: &raftpb.Snapshot{
			Data:     encodeSnapshotToken(55, crc),
			Metadata: testSnapshotMetadata(55, 2, nil),
		},
	}

	err := transport.streamFSMSnapshot(context.Background(), msg, 55, openFn)
	require.NoError(t, err)

	require.NotEmpty(t, sendClient.chunks)

	// Metadata must appear only in the first chunk.
	require.NotEmpty(t, sendClient.chunks[0].Metadata)
	for _, c := range sendClient.chunks[1:] {
		require.Empty(t, c.Metadata)
	}

	// Reconstruct and compare the streamed payload.
	var got []byte
	for _, c := range sendClient.chunks {
		got = append(got, c.Chunk...)
	}
	require.Equal(t, payload, got)
	require.True(t, sendClient.chunks[len(sendClient.chunks)-1].Final)
}

func TestStreamFSMSnapshotFileNotFound(t *testing.T) {
	dir := t.TempDir()

	sendClient := &testSnapshotSendClient{}
	transport := NewGRPCTransport([]Peer{{NodeID: 4, Address: "host:3"}})
	injectClient(t, transport, "host:3", &testEtcdRaftClient{stream: sendClient})

	openFn := func(index uint64) (io.ReadCloser, error) {
		return openFSMSnapshotPayloadReader(fsmSnapPath(dir, index))
	}

	msg := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		To:   uint64Ptr(4),
		Snapshot: &raftpb.Snapshot{
			Metadata: testSnapshotMetadata(999, 1, nil),
		},
	}

	err := transport.streamFSMSnapshot(context.Background(), msg, 999, openFn)
	require.ErrorIs(t, err, ErrFSMSnapshotNotFound)
	require.Empty(t, sendClient.chunks)
}

// --- dispatchSnapshot routing tests ---

func TestDispatchSnapshotTokenRoutesToStream(t *testing.T) {
	// When snapshot.Data is a token and openFSMPayload is set, dispatchSnapshot
	// must route to streamFSMSnapshot (chunked streaming path) — not bridge mode.
	dir := t.TempDir()
	payload := []byte("dispatch token route test payload data 12345")
	crc, _ := writeFSMFileForTest(t, dir, 42, payload)

	sendClient := &testSnapshotSendClient{}
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: "fake:1"}})
	injectClient(t, transport, "fake:1", &testEtcdRaftClient{stream: sendClient})
	transport.SetFSMPayloadOpener(func(index uint64) (io.ReadCloser, error) {
		return openFSMSnapshotPayloadReader(fsmSnapPath(dir, index))
	})

	msg := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Data:     encodeSnapshotToken(42, crc),
			Metadata: testSnapshotMetadata(42, 1, nil),
		},
	}

	err := transport.dispatchSnapshot(context.Background(), msg)
	require.NoError(t, err)

	// Chunks must carry the FSM payload (not the raw token bytes).
	var got []byte
	for _, c := range sendClient.chunks {
		got = append(got, c.Chunk...)
	}
	require.Equal(t, payload, got)
}

func TestDispatchSnapshotNonTokenRoutesToBridge(t *testing.T) {
	// When snapshot.Data is NOT a token (legacy full payload), dispatchSnapshot
	// must forward it unchanged via the bridge (sendSnapshot) path.
	legacy := []byte("legacy full fsm payload not a token")

	sendClient := &testSnapshotSendClient{}
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: "fake:1"}})
	injectClient(t, transport, "fake:1", &testEtcdRaftClient{stream: sendClient})

	msg := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Data:     legacy,
			Metadata: testSnapshotMetadata(1, 1, nil),
		},
	}

	err := transport.dispatchSnapshot(context.Background(), msg)
	require.NoError(t, err)

	// The legacy payload must reach the receiver unmodified.
	var got []byte
	for _, c := range sendClient.chunks {
		got = append(got, c.Chunk...)
	}
	require.Equal(t, legacy, got)
}

func TestDispatchSnapshotTokenNoOpenerFallsBackToBridge(t *testing.T) {
	// Token snapshot with NO openFSMPayload set → falls back to bridge mode.
	// Without a readFSMPayload either, applyBridgeMode is a passthrough, so
	// the raw token bytes are sent. This verifies the fallback branch is taken.
	dir := t.TempDir()
	payload := []byte("bridge fallback test payload data here")
	crc, _ := writeFSMFileForTest(t, dir, 77, payload)
	token := encodeSnapshotToken(77, crc)

	sendClient := &testSnapshotSendClient{}
	transport := NewGRPCTransport([]Peer{{NodeID: 2, Address: "fake:1"}})
	injectClient(t, transport, "fake:1", &testEtcdRaftClient{stream: sendClient})
	// No SetFSMPayloadOpener / SetFSMPayloadReader → passthrough bridge.

	msg := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgSnap),
		To:   uint64Ptr(2),
		Snapshot: &raftpb.Snapshot{
			Data:     token,
			Metadata: testSnapshotMetadata(77, 1, nil),
		},
	}

	err := transport.dispatchSnapshot(context.Background(), msg)
	require.NoError(t, err)

	// Token bytes forwarded as-is (no opener wired).
	var got []byte
	for _, c := range sendClient.chunks {
		got = append(got, c.Chunk...)
	}
	require.Equal(t, token, got)
}
