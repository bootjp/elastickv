package filesystem

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type writeOperationFaultDispatcher struct {
	inner          *testCoordinator
	key            []byte
	once           bool
	before         func(context.Context) error
	loseAck        bool
	loseRejection  bool
	rejectConflict bool
}

func (d *writeOperationFaultDispatcher) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if response, err, handled := d.handleRejection(ctx, req); handled {
		return response, err
	}
	if !d.once && writeOperationTouchesKey(req, d.key) {
		d.once = true
		if d.before != nil {
			if err := d.before(ctx); err != nil {
				return nil, err
			}
		}
		resp, err := d.inner.Dispatch(ctx, req)
		if err == nil && d.loseAck {
			return nil, kv.ErrTransactionOutcomeUnknown
		}
		return resp, err
	}
	return d.inner.Dispatch(ctx, req)
}

func (d *writeOperationFaultDispatcher) handleRejection(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error, bool) {
	if d.once || !isWriteOperationRejection(req) {
		return nil, nil, false
	}
	if d.rejectConflict {
		d.once = true
		return nil, store.ErrWriteConflict, true
	}
	if d.loseRejection {
		response, err := d.rejectWithLostReply(ctx, req)
		return response, err, true
	}
	return nil, nil, false
}

func (d *writeOperationFaultDispatcher) rejectWithLostReply(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	d.once = true
	if _, err := d.inner.Dispatch(ctx, req); err != nil {
		return nil, err
	}
	return nil, kv.ErrTransactionOutcomeUnknown
}

func isWriteOperationRejection(req *kv.OperationGroup[kv.OP]) bool {
	for _, elem := range req.Elems {
		if elem == nil || !fskeys.IsOperationKey(elem.Key) {
			continue
		}
		record, err := decodeOperationRecord(elem.Value)
		return err == nil && record.State == OperationRejected
	}
	return false
}

func writeOperationTouchesKey(req *kv.OperationGroup[kv.OP], key []byte) bool {
	for _, elem := range req.Elems {
		if elem != nil && string(elem.Key) == string(key) {
			return true
		}
	}
	return false
}

func testWriteOperationID(t *testing.T, svc *Service, sequence uint64) OperationID {
	t.Helper()
	journal, err := NewOperationJournal(svc)
	require.NoError(t, err)
	filesystemID, err := journal.EnsureFilesystem(t.Context())
	require.NoError(t, err)
	session, err := journal.RegisterSession(t.Context(), filesystemID, "write-test")
	require.NoError(t, err)
	return OperationID{Filesystem: filesystemID, Session: session.Session, Sequence: sequence}
}

func TestWriteWithOperationReturnsOriginalResultWithoutReplaying(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	n, err := svc.WriteWithOperation(ctx, id, file.Inode, 0, []byte("abcd"))
	require.NoError(t, err)
	require.Equal(t, 4, n)
	_, err = svc.Write(ctx, file.Inode, 0, 0, []byte("NEW!"))
	require.NoError(t, err)
	n, err = svc.WriteWithOperation(ctx, id, file.Inode, 0, []byte("abcd"))
	require.NoError(t, err)
	require.Equal(t, 4, n)
	data, err := svc.Read(ctx, file.Inode, 0, 0, 4)
	require.NoError(t, err)
	require.Equal(t, []byte("NEW!"), data)
	_, err = svc.WriteWithOperation(ctx, id, file.Inode, 0, []byte("different"))
	require.ErrorIs(t, err, ErrOperationIDReused)
}

func TestWriteWithOperationRetainsSemanticFailure(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, nil)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	_, err := svc.WriteWithOperation(ctx, id, RootInode, 0, []byte("x"))
	require.ErrorIs(t, err, ErrIsDir)
	_, err = svc.WriteWithOperation(ctx, id, RootInode, 0, []byte("x"))
	require.ErrorIs(t, err, ErrIsDir)
	journal, err := NewOperationJournal(svc)
	require.NoError(t, err)
	record, err := journal.Resolve(ctx, id)
	require.NoError(t, err)
	require.Equal(t, OperationRejected, record.State)
}

func TestWriteWithOperationRetainsMissingInodeRejection(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, nil)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	_, err := svc.WriteWithOperation(ctx, id, 99, 0, []byte("x"))
	require.ErrorIs(t, err, ErrNotFound)
	_, err = svc.WriteWithOperation(ctx, id, 99, 0, []byte("x"))
	require.ErrorIs(t, err, ErrNotFound)
}

func TestWriteOperationEffectLimitTracksChunkCount(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	one, err := svc.writeOperationEffectLimit(ctx, file.Inode, 0, 1)
	require.NoError(t, err)
	two, err := svc.writeOperationEffectLimit(ctx, file.Inode, 0, testChunkSize+1)
	require.NoError(t, err)
	require.Greater(t, two, one)
	require.Less(t, two, uint32(writeOperationMaxEffectLimit))
}

func TestWriteWithOperationDoesNotReplayPendingAttempt(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	journal, err := NewOperationJournal(svc)
	require.NoError(t, err)
	effectLimit, err := svc.writeOperationEffectLimit(ctx, file.Inode, 0, 1)
	require.NoError(t, err)
	_, err = journal.Admit(ctx, id, encodeWriteOperationIntent(file.Inode, 0, []byte("x")), writeOperationResultBytes, effectLimit)
	require.NoError(t, err)
	_, err = journal.BeginAttempt(ctx, id)
	require.NoError(t, err)
	_, err = svc.WriteWithOperation(ctx, id, file.Inode, 0, []byte("x"))
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	data, err := svc.Read(ctx, file.Inode, 0, 0, 1)
	require.NoError(t, err)
	require.Empty(t, data)
}

func TestWriteWithOperationReplansAfterDefinitiveConflict(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	fault := &writeOperationFaultDispatcher{inner: inner, key: fskeys.ChunkKey(file.Inode, file.Inode, 0)}
	fault.before = func(ctx context.Context) error {
		_, err := svc.Write(ctx, file.Inode, 0, 1, []byte("Z"))
		if err != nil {
			return err
		}
		return store.ErrWriteConflict
	}
	svc.dispatch = fault
	n, err := svc.WriteWithOperation(ctx, id, file.Inode, 0, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	data, err := svc.Read(ctx, file.Inode, 0, 0, 2)
	require.NoError(t, err)
	require.Equal(t, []byte("aZ"), data)
}

func TestWriteWithOperationResolvesLostCommitReply(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, []uint64{2})
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	file, err := svc.Create(ctx, RootInode, []byte("file"), CreateOptions{Mode: testFileMode})
	require.NoError(t, err)
	id := testWriteOperationID(t, svc, 1)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	svc.dispatch = &writeOperationFaultDispatcher{inner: inner,
		key: fskeys.ChunkKey(file.Inode, file.Inode, 0), loseAck: true}
	n, err := svc.WriteWithOperation(ctx, id, file.Inode, 0, []byte("x"))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	data, err := svc.Read(ctx, file.Inode, 0, 0, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("x"), data)
}

func TestWriteWithOperationResolvesLostRejectionReply(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, nil)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	svc.dispatch = &writeOperationFaultDispatcher{inner: inner, loseRejection: true}
	_, err := svc.WriteWithOperation(ctx, id, RootInode, 0, []byte("x"))
	require.ErrorIs(t, err, ErrIsDir)
	_, err = svc.WriteWithOperation(ctx, id, RootInode, 0, []byte("x"))
	require.ErrorIs(t, err, ErrIsDir)
}

func TestWriteWithOperationReplansDefinitiveRejectionConflict(t *testing.T) {
	ctx := context.Background()
	svc := newTestServiceWithOptions(t, nil)
	require.NoError(t, svc.InitializeRoot(ctx, testRootMode, 1000, 1000))
	id := testWriteOperationID(t, svc, 1)
	inner, ok := svc.dispatch.(*testCoordinator)
	require.True(t, ok)
	svc.dispatch = &writeOperationFaultDispatcher{inner: inner, rejectConflict: true}
	_, err := svc.WriteWithOperation(ctx, id, 999, 0, []byte("x"))
	require.ErrorIs(t, err, ErrNotFound)
	_, err = svc.WriteWithOperation(ctx, id, 999, 0, []byte("x"))
	require.ErrorIs(t, err, ErrNotFound)
}
