package fuseadapter

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

type acknowledgementCore struct {
	*receiptCore
	acknowledged chan filesystem.OperationID
	closed       bool
}

type lostAcknowledgementCore struct {
	*receiptCore
	retired bool
}

func (c *lostAcknowledgementCore) VerifyOperationReceipt(context.Context, filesystem.OperationID) error {
	if c.retired {
		return filesystem.ErrOperationRetired
	}
	return nil
}

func (c *lostAcknowledgementCore) AcknowledgeOperation(context.Context, filesystem.OperationID) error {
	if !c.retired {
		c.retired = true
		return context.DeadlineExceeded
	}
	return nil
}

func (c *acknowledgementCore) AcknowledgeOperation(_ context.Context, id filesystem.OperationID) error {
	c.acknowledged <- id
	return nil
}

func (c *acknowledgementCore) CloseOperationSession(_ context.Context, _ filesystem.OperationSessionID) error {
	select {
	case <-c.acknowledged:
		c.closed = true
		return nil
	default:
		return filesystem.ErrOperationPending
	}
}

func newAcknowledgementCore() *acknowledgementCore {
	return &acknowledgementCore{
		receiptCore: &receiptCore{journalMetadataCore: &journalMetadataCore{
			journalWriteCore: &journalWriteCore{fakeCore: &fakeCore{}},
		}},
		acknowledged: make(chan filesystem.OperationID, 1),
	}
}

func TestAcknowledgementAcceptsRetiredReceipt(t *testing.T) {
	core := newAcknowledgementCore()
	core.verifyErr = filesystem.ErrOperationRetired
	adapter := New(core, []byte("client"), WithHandleKeepaliveInterval(0))
	id := filesystem.OperationID{Filesystem: [16]byte{1}, Session: [16]byte{2}, Sequence: 1}
	adapter.recordReceipt(7, id)
	require.NoError(t, adapter.acknowledgeReceipt(t.Context(), id))
	require.Empty(t, adapter.receipts)
	select {
	case got := <-core.acknowledged:
		require.Equal(t, id, got)
	default:
		t.Fatal("operation was not acknowledged")
	}
	adapter.recordReceipt(7, id)
	require.Zero(t, adapter.Fsync(t.Context(), 7, 9, false))
	require.True(t, core.fsyncCalled)
}

func TestAcknowledgementRetriesAfterLostResponse(t *testing.T) {
	core := &lostAcknowledgementCore{receiptCore: newAcknowledgementCore().receiptCore}
	adapter := New(core, []byte("client"), WithHandleKeepaliveInterval(0))
	id := filesystem.OperationID{Filesystem: [16]byte{1}, Session: [16]byte{2}, Sequence: 1}
	adapter.recordReceipt(7, id)
	require.ErrorIs(t, adapter.acknowledgeReceipt(t.Context(), id), context.DeadlineExceeded)
	require.Contains(t, adapter.receipts[7], id)
	require.NoError(t, adapter.acknowledgeReceipt(t.Context(), id))
	require.Empty(t, adapter.receipts)
}

type blockingAcknowledgementCore struct {
	*acknowledgementCore
	verifying chan struct{}
	release   chan struct{}
}

func (c *blockingAcknowledgementCore) VerifyOperationReceipt(_ context.Context, _ filesystem.OperationID) error {
	close(c.verifying)
	<-c.release
	return nil
}

func TestAcknowledgementDoesNotHoldReceiptLockDuringVerification(t *testing.T) {
	core := &blockingAcknowledgementCore{
		acknowledgementCore: newAcknowledgementCore(),
		verifying:           make(chan struct{}),
		release:             make(chan struct{}),
	}
	adapter := New(core, []byte("client"), WithHandleKeepaliveInterval(0))
	id := filesystem.OperationID{Filesystem: [16]byte{1}, Session: [16]byte{2}, Sequence: 1}
	adapter.recordReceipt(7, id)
	done := make(chan error, 1)
	go func() { done <- adapter.acknowledgeReceipt(t.Context(), id) }()
	select {
	case <-core.verifying:
	case <-time.After(time.Second):
		t.Fatal("verification did not start")
	}
	recorded := make(chan struct{})
	go func() {
		adapter.recordReceipt(8, filesystem.OperationID{Filesystem: id.Filesystem, Session: id.Session, Sequence: 2})
		close(recorded)
	}()
	select {
	case <-recorded:
	case <-time.After(time.Second):
		t.Fatal("receipt lock held during verification")
	}
	close(core.release)
	require.NoError(t, <-done)
}

func TestFsyncDoesNotHoldReceiptLockDuringVerification(t *testing.T) {
	core := &blockingAcknowledgementCore{
		acknowledgementCore: newAcknowledgementCore(),
		verifying:           make(chan struct{}),
		release:             make(chan struct{}),
	}
	adapter := New(core, []byte("client"), WithHandleKeepaliveInterval(0))
	id := filesystem.OperationID{Filesystem: [16]byte{1}, Session: [16]byte{2}, Sequence: 1}
	adapter.recordReceipt(7, id)
	done := make(chan syscall.Errno, 1)
	go func() { done <- adapter.Fsync(t.Context(), 7, 9, false) }()
	select {
	case <-core.verifying:
	case <-time.After(time.Second):
		t.Fatal("verification did not start")
	}
	recorded := make(chan struct{})
	go func() {
		adapter.recordReceipt(8, filesystem.OperationID{Filesystem: id.Filesystem, Session: id.Session, Sequence: 2})
		close(recorded)
	}()
	select {
	case <-recorded:
	case <-time.After(time.Second):
		t.Fatal("receipt lock held during fsync verification")
	}
	close(core.release)
	require.Zero(t, <-done)
}

func TestStrictCloseDrainsAcknowledgements(t *testing.T) {
	core := newAcknowledgementCore()
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	adapter := New(core, []byte("client"), WithHandleKeepaliveInterval(0), WithStrictMode(session))
	adapter.queueAcknowledgement(filesystem.OperationID{Filesystem: session.Filesystem, Session: session.Session, Sequence: 1})
	adapter.Close()
	require.True(t, core.closed)
}

func TestAcknowledgementBatchRotatesPastFailures(t *testing.T) {
	adapter := New(&fakeCore{}, []byte("client"), WithHandleKeepaliveInterval(0))
	adapter.ackPending = make(map[filesystem.OperationID]struct{})
	for sequence := uint64(1); sequence <= 70; sequence++ {
		adapter.ackPending[filesystem.OperationID{
			Filesystem: [16]byte{1}, Session: [16]byte{2}, Sequence: sequence,
		}] = struct{}{}
	}
	first := adapter.pendingAcknowledgements()
	second := adapter.pendingAcknowledgements()
	require.Len(t, first, ackBatchSize)
	require.EqualValues(t, 1, first[0].Sequence)
	require.EqualValues(t, 64, first[len(first)-1].Sequence)
	require.EqualValues(t, 65, second[0].Sequence)
	require.EqualValues(t, 70, second[5].Sequence)
}

func TestSuppressedReplyQueuesOperationIdentity(t *testing.T) {
	adapter := New(&fakeCore{}, []byte("client"), WithHandleKeepaliveInterval(0))
	fs := NewRawFileSystem(adapter)
	id := filesystem.OperationID{Filesystem: [16]byte{1}, Session: [16]byte{2}, Sequence: 1}
	adapter.recordReplyIdentity(withReplyIdentity(t.Context(), 42), id)
	fs.OnReplySuppressed(42, 0, fuse.EINTR)
	require.Empty(t, adapter.replyIDs)
	require.Contains(t, adapter.ackPending, id)
}

func TestStrictSessionWaitsForUnansweredRequest(t *testing.T) {
	core := newAcknowledgementCore()
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	adapter := New(core, []byte("client"), WithHandleKeepaliveInterval(0), WithStrictMode(session))
	t.Cleanup(adapter.Close)
	id := filesystem.OperationID{Filesystem: session.Filesystem, Session: session.Session, Sequence: 1}
	adapter.recordReplyIdentity(withReplyIdentity(t.Context(), 42), id)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, adapter.finishStrictSession(ctx), context.DeadlineExceeded)
	require.False(t, core.closed)
	adapter.replySuppressed(42)
	ctx, cancel = context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, adapter.finishStrictSession(ctx))
	require.True(t, core.closed)
}
