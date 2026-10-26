package fuseadapter

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func newIdleAdapter(t *testing.T, core Core) *Adapter {
	t.Helper()
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	return adapter
}

func TestAdapterSetAttrForwardsMaskAndMapsErrors(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{setAttrStat: filesystem.Stat{Inode: 7, Size: 3}}
	adapter := newIdleAdapter(t, core)

	mask := filesystem.SetAttrMask{Size: true, Mode: true}
	attrs := filesystem.SetAttr{Size: 3, Mode: 0o600}
	stat, errno := adapter.SetAttr(ctx, 7, mask, attrs)
	require.Zero(t, errno)
	require.EqualValues(t, 3, stat.Size)
	require.EqualValues(t, 7, core.setAttrInode)
	require.Equal(t, mask, core.setAttrMask)
	require.Equal(t, attrs, core.setAttrAttrs)

	core.setAttrErr = filesystem.ErrIsDir
	_, errno = adapter.SetAttr(ctx, 7, mask, attrs)
	require.Equal(t, syscall.EISDIR, errno)
}

func TestAdapterWriteRefreshesLeaseThenForwards(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{}
	adapter := newIdleAdapter(t, core)

	n, errno := adapter.Write(ctx, 7, 9, 4, []byte("data"))
	require.Zero(t, errno)
	require.Equal(t, 4, n)
	require.EqualValues(t, 7, core.refreshInode)
	require.EqualValues(t, 9, core.refreshFH)
	require.Equal(t, []byte("fuse-session"), core.refreshClientID)
	require.EqualValues(t, 7, core.writeInode)
	require.EqualValues(t, 9, core.writeFH)
	require.EqualValues(t, 4, core.writeOffset)
	require.Equal(t, []byte("data"), core.writeData)
}

type journalWriteCore struct {
	*fakeCore
	ids []filesystem.OperationID
}

func (c *journalWriteCore) WriteWithOperation(_ context.Context, id filesystem.OperationID, _ uint64, _ uint64, data []byte) (int, error) {
	c.ids = append(c.ids, id)
	return len(data), nil
}

type journalMetadataCore struct {
	*journalWriteCore
	metadataIDs []filesystem.OperationID
}

type receiptCore struct {
	*journalMetadataCore
	verified  []filesystem.OperationID
	verifyErr error
}

type journalUnlinkCore struct {
	*receiptCore
	unlinkIDs []filesystem.OperationID
}

type journalLifecycleCore struct {
	*receiptCore
	createID  filesystem.OperationID
	openID    filesystem.OperationID
	openErr   error
	releaseID filesystem.OperationID
}

type journalLeaseCore struct {
	*receiptCore
	leaseIDs []filesystem.OperationID
	leaseErr error
}

type validatingLeaseCore struct {
	*journalLeaseCore
	ttl           time.Duration
	validationErr error
	validations   int
}

func (c *validatingLeaseCore) OpenHandleLeaseTTL() time.Duration { return c.ttl }

func (c *validatingLeaseCore) ValidateOpenHandleLease(context.Context, uint64, uint64, []byte) error {
	c.validations++
	return c.validationErr
}

func TestAdapterReadCoalescesJournaledLeaseRefresh(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &validatingLeaseCore{ttl: 5 * time.Minute, journalLeaseCore: &journalLeaseCore{
		receiptCore: &receiptCore{journalMetadataCore: &journalMetadataCore{
			journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}},
		}},
	}}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0), WithJournaledLeaseRefresh(session))
	t.Cleanup(adapter.Close)
	for range 100 {
		_, errno := adapter.Read(t.Context(), 7, 9, 0, 1)
		require.Zero(t, errno)
	}
	require.Len(t, core.leaseIDs, 1)
	require.Equal(t, 99, core.validations)
	adapter.handlesMu.Lock()
	adapter.leaseRefreshed[openHandleKey{inode: 7, fh: 9}] = time.Now().Add(-2 * time.Minute)
	adapter.handlesMu.Unlock()
	_, errno := adapter.Read(t.Context(), 7, 9, 0, 1)
	require.Zero(t, errno)
	require.Len(t, core.leaseIDs, 2)
	require.Equal(t, []filesystem.OperationID{core.leaseIDs[0]}, core.verified)
	adapter.receiptMu.Lock()
	require.Len(t, adapter.receipts[7], 1)
	adapter.receiptMu.Unlock()
	core.validationErr = filesystem.ErrNotFound
	_, errno = adapter.Read(t.Context(), 7, 9, 0, 1)
	require.Equal(t, syscall.ENOENT, errno)
	require.Len(t, core.leaseIDs, 2)
}

func (c *journalLeaseCore) RefreshOpenHandleLeaseWithOperation(_ context.Context, id filesystem.OperationID, _ uint64, _ uint64, _ []byte) error {
	c.leaseIDs = append(c.leaseIDs, id)
	return c.leaseErr
}

func TestAdapterJournaledLeaseRefreshPrecedesFsync(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &journalLeaseCore{receiptCore: &receiptCore{journalMetadataCore: &journalMetadataCore{
		journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}},
	}}}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0), WithJournaledLeaseRefresh(session))
	t.Cleanup(adapter.Close)
	_, errno := adapter.Read(t.Context(), 7, 9, 0, 1)
	require.Zero(t, errno)
	require.Zero(t, adapter.Fsync(t.Context(), 7, 9, false))
	require.ElementsMatch(t, core.leaseIDs, core.verified)
	require.Len(t, core.leaseIDs, 2)
	require.True(t, core.fsyncCalled)
}

func TestAdapterUnknownLeaseRefreshStopsLaterFsync(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &journalLeaseCore{receiptCore: &receiptCore{journalMetadataCore: &journalMetadataCore{
		journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}},
	}}, leaseErr: kv.ErrTransactionOutcomeUnknown}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0), WithJournaledLeaseRefresh(session))
	t.Cleanup(adapter.Close)
	_, errno := adapter.Read(t.Context(), 7, 9, 0, 1)
	require.Equal(t, syscall.EIO, errno)
	require.Equal(t, syscall.EIO, adapter.Fsync(t.Context(), 7, 9, false))
	require.False(t, core.fsyncCalled)
}

func TestAdapterKeepaliveUsesJournaledLeaseRefresh(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &journalLeaseCore{receiptCore: &receiptCore{journalMetadataCore: &journalMetadataCore{
		journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}},
	}}}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0), WithJournaledLeaseRefresh(session))
	t.Cleanup(adapter.Close)
	adapter.trackOpenHandle(7, 9)
	adapter.refreshTrackedOpenHandles(t.Context())
	require.Len(t, core.leaseIDs, 1)
	adapter.replyMu.Lock()
	_, queued := adapter.ackPending[core.leaseIDs[0]]
	adapter.replyMu.Unlock()
	require.True(t, queued)
	require.Zero(t, adapter.Fsync(t.Context(), 7, 9, false))
	require.ElementsMatch(t, core.leaseIDs, core.verified)
}

func TestAdapterKeepaliveRetainsUnknownLease(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &journalLeaseCore{receiptCore: &receiptCore{journalMetadataCore: &journalMetadataCore{
		journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}},
	}}, leaseErr: kv.ErrTransactionOutcomeUnknown}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0), WithJournaledLeaseRefresh(session))
	t.Cleanup(adapter.Close)
	adapter.trackOpenHandle(7, 9)
	adapter.refreshTrackedOpenHandles(t.Context())
	require.Len(t, core.leaseIDs, 1)
	adapter.replyMu.Lock()
	_, queued := adapter.ackPending[core.leaseIDs[0]]
	adapter.replyMu.Unlock()
	require.False(t, queued)
}

func (c *journalLifecycleCore) CreateWithOperation(_ context.Context, id filesystem.OperationID, _ uint64, _ []byte, _ filesystem.CreateOptions) (filesystem.CreateResult, error) {
	c.createID = id
	return filesystem.CreateResult{Inode: 7, FH: 9, Stat: filesystem.Stat{Inode: 7}}, nil
}

func (c *journalLifecycleCore) OpenWithOperation(_ context.Context, id filesystem.OperationID, _ uint64, _ []byte) (uint64, error) {
	c.openID = id
	return 10, c.openErr
}

func TestAdapterUnknownOpenStopsLaterFsync(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &journalLifecycleCore{receiptCore: &receiptCore{journalMetadataCore: &journalMetadataCore{
		journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}},
	}}, openErr: kv.ErrTransactionOutcomeUnknown}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0), WithJournaledOpen(session))
	t.Cleanup(adapter.Close)
	_, errno := adapter.Open(t.Context(), 7)
	require.Equal(t, syscall.EIO, errno)
	require.Equal(t, syscall.EIO, adapter.Fsync(t.Context(), 7, 9, false))
	require.False(t, core.fsyncCalled)
}

func (c *journalLifecycleCore) ReleaseWithOperation(_ context.Context, id filesystem.OperationID, _ uint64, _ uint64, _ []byte) error {
	c.releaseID = id
	return nil
}

func TestAdapterFsyncVerifiesCreateOpenAndReleaseReceipts(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &journalLifecycleCore{receiptCore: &receiptCore{journalMetadataCore: &journalMetadataCore{
		journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}},
	}}}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0),
		WithJournaledCreate(session), WithJournaledOpen(session), WithJournaledRelease(session))
	t.Cleanup(adapter.Close)
	result, errno := adapter.Create(t.Context(), filesystem.RootInode, []byte("file"), filesystem.CreateOptions{Mode: 0o600})
	require.Zero(t, errno)
	fh, errno := adapter.Open(t.Context(), result.Inode)
	require.Zero(t, errno)
	require.Zero(t, adapter.Release(t.Context(), result.Inode, fh))
	require.Zero(t, adapter.Fsync(t.Context(), result.Inode, result.FH, false))
	require.ElementsMatch(t, []filesystem.OperationID{core.createID, core.openID, core.releaseID}, core.verified)
	core.verified = nil
	require.Zero(t, adapter.FsyncDirectory(t.Context(), filesystem.RootInode))
	require.Equal(t, []filesystem.OperationID{core.createID}, core.verified)
}

func (c *journalUnlinkCore) UnlinkWithOperation(_ context.Context, id filesystem.OperationID, _ uint64, _ []byte) error {
	c.unlinkIDs = append(c.unlinkIDs, id)
	return nil
}

func TestAdapterDirectoryFsyncVerifiesUnlinkReceipt(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &journalUnlinkCore{receiptCore: &receiptCore{journalMetadataCore: &journalMetadataCore{
		journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}},
	}}}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0), WithJournaledUnlink(session))
	t.Cleanup(adapter.Close)
	require.Zero(t, adapter.Unlink(t.Context(), filesystem.RootInode, []byte("file")))
	require.Zero(t, adapter.FsyncDirectory(t.Context(), filesystem.RootInode))
	require.Equal(t, core.unlinkIDs, core.verified)
	require.True(t, core.fsyncCalled)
	require.Zero(t, core.unlinkParent)
}

func (c *receiptCore) VerifyOperationReceipt(_ context.Context, id filesystem.OperationID) error {
	c.verified = append(c.verified, id)
	return c.verifyErr
}

func TestAdapterFsyncVerifiesJournalReceipts(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &receiptCore{journalMetadataCore: &journalMetadataCore{
		journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}},
	}}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0),
		WithJournaledWrites(session), WithJournaledMetadata(session))
	t.Cleanup(adapter.Close)
	_, errno := adapter.Write(t.Context(), 7, 9, 0, []byte("x"))
	require.Zero(t, errno)
	_, errno = adapter.SetAttr(t.Context(), 7, filesystem.SetAttrMask{Mode: true}, filesystem.SetAttr{Mode: 0o600})
	require.Zero(t, errno)
	require.Zero(t, adapter.Fsync(t.Context(), 7, 9, false))
	require.ElementsMatch(t, []filesystem.OperationID{core.ids[0], core.metadataIDs[0]}, core.verified)
	require.True(t, core.fsyncCalled)
	core.verified = nil
	require.Zero(t, adapter.Fsync(t.Context(), 7, 9, false))
	require.Empty(t, core.verified)
}

func TestAdapterFsyncFailsClosedOnUnverifiedReceipt(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &receiptCore{journalMetadataCore: &journalMetadataCore{
		journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}},
	}, verifyErr: filesystem.ErrOperationPending}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0), WithJournaledWrites(session))
	t.Cleanup(adapter.Close)
	_, errno := adapter.Write(t.Context(), 7, 9, 0, []byte("x"))
	require.Zero(t, errno)
	require.Equal(t, syscall.EIO, adapter.Fsync(t.Context(), 7, 9, false))
	require.False(t, core.fsyncCalled)
	require.Equal(t, syscall.EIO, adapter.Flush(t.Context(), 7, 9))
	require.False(t, core.flushCalled)
}

func (c *journalMetadataCore) SetAttrWithOperation(_ context.Context, id filesystem.OperationID, inode uint64, _ filesystem.SetAttrMask, _ filesystem.SetAttr) (filesystem.Stat, error) {
	c.metadataIDs = append(c.metadataIDs, id)
	return filesystem.Stat{Inode: inode}, nil
}

func TestAdapterJournaledMetadataSharesWriteSequence(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &journalMetadataCore{journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}}}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0),
		WithJournaledWrites(session), WithJournaledMetadata(session))
	t.Cleanup(adapter.Close)
	_, errno := adapter.SetAttr(t.Context(), 7, filesystem.SetAttrMask{Mode: true}, filesystem.SetAttr{Mode: 0o600})
	require.Zero(t, errno)
	_, errno = adapter.Write(t.Context(), 7, 9, 0, []byte("x"))
	require.Zero(t, errno)
	require.EqualValues(t, 1, core.metadataIDs[0].Sequence)
	require.EqualValues(t, 2, core.ids[0].Sequence)
	require.Zero(t, core.setAttrInode)
}

func TestAdapterRejectsDifferentJournalSessions(t *testing.T) {
	first := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	second := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{3}}
	core := &journalMetadataCore{journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}}}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0),
		WithJournaledWrites(first), WithJournaledMetadata(second))
	t.Cleanup(adapter.Close)
	_, errno := adapter.Write(t.Context(), 7, 9, 0, []byte("x"))
	require.Equal(t, syscall.EOPNOTSUPP, errno)
	_, errno = adapter.SetAttr(t.Context(), 7, filesystem.SetAttrMask{Mode: true}, filesystem.SetAttr{Mode: 0o600})
	require.Equal(t, syscall.EOPNOTSUPP, errno)
	require.Empty(t, core.ids)
	require.Empty(t, core.metadataIDs)
}

func TestAdapterJournaledWritesAllocateStableSessionSequence(t *testing.T) {
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	core := &journalWriteCore{fakeCore: &fakeCore{}}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(0), WithJournaledWrites(session))
	t.Cleanup(adapter.Close)
	n, errno := adapter.Write(t.Context(), 7, 9, 0, nil)
	require.Zero(t, errno)
	require.Zero(t, n)
	for range 2 {
		n, errno = adapter.Write(t.Context(), 7, 9, 0, []byte("x"))
		require.Zero(t, errno)
		require.Equal(t, 1, n)
	}
	require.False(t, core.writeCalled)
	require.Equal(t, []filesystem.OperationID{
		{Filesystem: session.Filesystem, Session: session.Session, Sequence: 1},
		{Filesystem: session.Filesystem, Session: session.Session, Sequence: 2},
	}, core.ids)
}

func TestAdapterWriteStopsWhenLeaseRefreshFails(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{refreshErr: filesystem.ErrStaleHome}
	adapter := newIdleAdapter(t, core)

	n, errno := adapter.Write(ctx, 7, 9, 0, []byte("data"))
	require.Equal(t, syscall.EAGAIN, errno)
	require.Zero(t, n)
	require.False(t, core.writeCalled)
}

func TestAdapterWriteMapsBackendError(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{writeErr: filesystem.ErrInvalid}
	adapter := newIdleAdapter(t, core)

	n, errno := adapter.Write(ctx, 7, 9, 0, []byte("data"))
	require.Equal(t, syscall.EINVAL, errno)
	require.Zero(t, n)
	require.True(t, core.writeCalled)
}

func TestAdapterFlushAndFsyncRefreshLeaseFirst(t *testing.T) {
	ctx := context.Background()

	t.Run("flush forwards", func(t *testing.T) {
		core := &fakeCore{}
		adapter := newIdleAdapter(t, core)
		require.Zero(t, adapter.Flush(ctx, 7, 9))
		require.True(t, core.flushCalled)
		require.EqualValues(t, 7, core.flushInode)
		require.EqualValues(t, 9, core.flushFH)
		require.EqualValues(t, 9, core.refreshFH)
	})

	t.Run("flush stops on stale lease", func(t *testing.T) {
		core := &fakeCore{refreshErr: filesystem.ErrNotFound}
		adapter := newIdleAdapter(t, core)
		require.Equal(t, syscall.ENOENT, adapter.Flush(ctx, 7, 9))
		require.False(t, core.flushCalled)
	})

	t.Run("fsync forwards datasync", func(t *testing.T) {
		core := &fakeCore{}
		adapter := newIdleAdapter(t, core)
		require.Zero(t, adapter.Fsync(ctx, 7, 9, true))
		require.True(t, core.fsyncCalled)
		require.True(t, core.fsyncDatasync)
		require.EqualValues(t, 7, core.fsyncInode)
		require.EqualValues(t, 9, core.fsyncFH)
	})

	t.Run("fsync maps backend error", func(t *testing.T) {
		core := &fakeCore{fsyncErr: errors.New("boom")}
		adapter := newIdleAdapter(t, core)
		require.Equal(t, syscall.EIO, adapter.Fsync(ctx, 7, 9, false))
	})

	t.Run("fsync stops on stale lease", func(t *testing.T) {
		core := &fakeCore{refreshErr: filesystem.ErrNotFound}
		adapter := newIdleAdapter(t, core)
		require.Equal(t, syscall.ENOENT, adapter.Fsync(ctx, 7, 9, false))
		require.False(t, core.fsyncCalled)
	})
}

func TestAdapterOpenDoesNotTrackFailedHandles(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{openErr: filesystem.ErrNotFound}
	adapter := newIdleAdapter(t, core)

	fh, errno := adapter.Open(ctx, 7)
	require.Equal(t, syscall.ENOENT, errno)
	require.Zero(t, fh)
	require.Empty(t, adapter.openHandleSnapshot())
}

func TestAdapterCreateDoesNotTrackFailedHandles(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{createErr: filesystem.ErrExists}
	adapter := newIdleAdapter(t, core)

	_, errno := adapter.Create(ctx, filesystem.RootInode, []byte("file"), filesystem.CreateOptions{})
	require.Equal(t, syscall.EEXIST, errno)
	require.Empty(t, adapter.openHandleSnapshot())
}

func TestAdapterReleaseUntracksHandleEvenWhenBackendFails(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{openFH: 9, releaseErr: filesystem.ErrNotFound}
	adapter := newIdleAdapter(t, core)

	_, errno := adapter.Open(ctx, 7)
	require.Zero(t, errno)
	require.Len(t, adapter.openHandleSnapshot(), 1)

	require.Equal(t, syscall.ENOENT, adapter.Release(ctx, 7, 9))
	require.Empty(t, adapter.openHandleSnapshot())
}

func TestAdapterMkdirForwardsOptionsWithoutClientID(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{mkdirResult: filesystem.CreateResult{Inode: 11}}
	adapter := newIdleAdapter(t, core)

	result, errno := adapter.Mkdir(ctx, filesystem.RootInode, []byte("dir"), filesystem.CreateOptions{
		Mode: 0o755,
		UID:  1000,
		GID:  1001,
	})
	require.Zero(t, errno)
	require.EqualValues(t, 11, result.Inode)
	require.EqualValues(t, filesystem.RootInode, core.mkdirParent)
	require.Equal(t, []byte("dir"), core.mkdirName)
	require.EqualValues(t, 0o755, core.mkdirOpts.Mode)
	require.EqualValues(t, 1000, core.mkdirOpts.UID)
	require.EqualValues(t, 1001, core.mkdirOpts.GID)
	require.Empty(t, core.mkdirOpts.ClientID)
	require.Empty(t, adapter.openHandleSnapshot())

	core.mkdirErr = filesystem.ErrExists
	_, errno = adapter.Mkdir(ctx, filesystem.RootInode, []byte("dir"), filesystem.CreateOptions{})
	require.Equal(t, syscall.EEXIST, errno)
}

func TestAdapterNamespaceMutationsForwardAndMapErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("unlink", func(t *testing.T) {
		core := &fakeCore{}
		adapter := newIdleAdapter(t, core)
		require.Zero(t, adapter.Unlink(ctx, 5, []byte("file")))
		require.EqualValues(t, 5, core.unlinkParent)
		require.Equal(t, []byte("file"), core.unlinkName)

		core.unlinkErr = filesystem.ErrIsDir
		require.Equal(t, syscall.EISDIR, adapter.Unlink(ctx, 5, []byte("file")))
	})

	t.Run("rmdir", func(t *testing.T) {
		core := &fakeCore{}
		adapter := newIdleAdapter(t, core)
		require.Zero(t, adapter.Rmdir(ctx, 5, []byte("dir")))
		require.EqualValues(t, 5, core.rmdirParent)
		require.Equal(t, []byte("dir"), core.rmdirName)

		core.rmdirErr = filesystem.ErrNotEmpty
		require.Equal(t, syscall.ENOTEMPTY, adapter.Rmdir(ctx, 5, []byte("dir")))
	})

	t.Run("rename", func(t *testing.T) {
		core := &fakeCore{}
		adapter := newIdleAdapter(t, core)
		require.Zero(t, adapter.Rename(ctx, 5, []byte("old"), 6, []byte("new")))
		require.Equal(t, renameCall{
			oldParent: 5,
			oldName:   []byte("old"),
			newParent: 6,
			newName:   []byte("new"),
		}, core.rename)

		core.renameErr = filesystem.ErrCrossDevice
		require.Equal(t, syscall.EXDEV, adapter.Rename(ctx, 5, []byte("old"), 6, []byte("new")))
	})
}

func TestAdapterGetAttrAndStatFSMapErrors(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{
		stat:       filesystem.Stat{Inode: 7},
		getAttrErr: filesystem.ErrNotFound,
		statFS:     filesystem.StatFS{Files: 3},
		statFSErr:  errors.New("boom"),
	}
	adapter := newIdleAdapter(t, core)

	_, errno := adapter.GetAttr(ctx, 7)
	require.Equal(t, syscall.ENOENT, errno)
	require.EqualValues(t, 7, core.getAttrInode)

	_, errno = adapter.StatFS(ctx, filesystem.RootInode)
	require.Equal(t, syscall.EIO, errno)

	core.getAttrErr = nil
	core.statFSErr = nil
	stat, errno := adapter.GetAttr(ctx, 7)
	require.Zero(t, errno)
	require.EqualValues(t, 7, stat.Inode)
	fsStat, errno := adapter.StatFS(ctx, filesystem.RootInode)
	require.Zero(t, errno)
	require.EqualValues(t, 3, fsStat.Files)
}

func TestAdapterLookupMapsResolveAndGetAttrErrors(t *testing.T) {
	ctx := context.Background()

	core := &fakeCore{resolveErr: filesystem.ErrNotFound}
	adapter := newIdleAdapter(t, core)
	_, errno := adapter.Lookup(ctx, filesystem.RootInode, []byte("missing"))
	require.Equal(t, syscall.ENOENT, errno)
	require.Zero(t, core.getAttrInode)

	core = &fakeCore{resolveInode: 42, getAttrErr: filesystem.ErrStaleHome}
	adapter = newIdleAdapter(t, core)
	_, errno = adapter.Lookup(ctx, filesystem.RootInode, []byte("moving"))
	require.Equal(t, syscall.EAGAIN, errno)
	require.EqualValues(t, 42, core.getAttrInode)
}

func TestAdapterReaddirMapsErrors(t *testing.T) {
	ctx := context.Background()
	core := &fakeCore{readdirErr: filesystem.ErrNotDir}
	adapter := newIdleAdapter(t, core)

	_, errno := adapter.Readdir(ctx, 7, "")
	require.Equal(t, syscall.ENOTDIR, errno)
	require.Equal(t, defaultReaddirLimit, core.readdirLimit)
}

func TestAdapterOptionsIgnoreNonPositiveValues(t *testing.T) {
	core := &fakeCore{}
	adapter := New(core, []byte("fuse-session"),
		WithReaddirLimit(0),
		WithReaddirLimit(-1),
		WithHandleKeepaliveTimeout(0),
		WithHandleKeepaliveInterval(-time.Second),
	)
	t.Cleanup(adapter.Close)

	require.Equal(t, defaultReaddirLimit, adapter.readdirLimit)
	require.Equal(t, defaultHandleKeepaliveTimeout, adapter.keepaliveTimeout)
	require.Nil(t, adapter.keepaliveCancel)
}

func TestAdapterCloseIsIdempotent(t *testing.T) {
	adapter := New(&fakeCore{}, []byte("fuse-session"), WithHandleKeepaliveInterval(time.Hour))
	require.NotNil(t, adapter.keepaliveCancel)
	adapter.Close()
	adapter.Close()
}

func TestAdapterKeepaliveStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	core := &fakeCore{refreshErr: errors.New("boom")}
	adapter := newIdleAdapter(t, core)
	adapter.trackOpenHandle(7, 1)
	adapter.trackOpenHandle(8, 2)

	cancel()
	adapter.refreshTrackedOpenHandles(ctx)

	require.Len(t, adapter.openHandleSnapshot(), 2)
}

func TestAdapterTrackingIgnoresZeroHandles(t *testing.T) {
	adapter := newIdleAdapter(t, &fakeCore{})
	adapter.trackOpenHandle(7, 0)
	require.Empty(t, adapter.openHandleSnapshot())
	adapter.untrackOpenHandle(7, 0)
	require.Empty(t, adapter.openHandleSnapshot())
}

func TestAdapterKeepaliveLoopRefreshesUntilClosed(t *testing.T) {
	core := &fakeCore{refreshCh: make(chan openHandleRefresh, 8)}
	adapter := New(core, []byte("fuse-session"), WithHandleKeepaliveInterval(5*time.Millisecond))
	adapter.trackOpenHandle(7, 9)

	select {
	case got := <-core.refreshCh:
		require.EqualValues(t, 9, got.fh)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for keepalive tick")
	}

	adapter.Close()
	drainDeadline := time.After(50 * time.Millisecond)
drain:
	for {
		select {
		case <-core.refreshCh:
		case <-drainDeadline:
			break drain
		}
	}
	select {
	case got := <-core.refreshCh:
		t.Fatalf("keepalive continued after Close: %+v", got)
	case <-time.After(30 * time.Millisecond):
	}
}
