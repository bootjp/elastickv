package fuseadapter

import (
	"context"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/kv"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestLegacyCloseRetainsUnknownWriteEvenAfterSuccessfulRead(t *testing.T) {
	t.Parallel()
	core := &fakeCore{writeErr: kv.ErrTransactionOutcomeUnknown, readData: []byte("15")}
	raw, _ := newRawFileSystem(t, core)
	_, status := raw.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9}, []byte("15"))
	require.Equal(t, fuse.Status(syscall.EIO), status)
	result, status := raw.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 10, Size: 2}, nil)
	require.Equal(t, fuse.OK, status)
	result.Done()
	require.Equal(t, fuse.Status(syscall.EIO), raw.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 10}))
}

type closeRecoveryCore struct {
	*receiptCore
	terminal atomic.Bool
	writes   atomic.Int32
}

func (c *closeRecoveryCore) WriteWithOperation(context.Context, filesystem.OperationID, uint64, uint64, []byte) (int, error) {
	c.writes.Add(1)
	return 0, kv.ErrTransactionOutcomeUnknown
}

func (c *closeRecoveryCore) RefreshOpenHandleLeaseWithOperation(context.Context, filesystem.OperationID, uint64, uint64, []byte) error {
	return nil
}

func (c *closeRecoveryCore) VerifyOperationReceipt(context.Context, filesystem.OperationID) error {
	if !c.terminal.Load() {
		return filesystem.ErrOperationPending
	}
	return nil
}

func (c *closeRecoveryCore) AcknowledgeOperation(context.Context, filesystem.OperationID) error {
	return nil
}
func (c *closeRecoveryCore) CloseOperationSession(context.Context, filesystem.OperationSessionID) error {
	return nil
}

func TestStrictCloseRecoversOnlyAfterUnknownWriteHasVerifiedDecision(t *testing.T) {
	t.Parallel()
	core := &closeRecoveryCore{receiptCore: newAcknowledgementCore().receiptCore}
	session := filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
	adapter := New(core, []byte("mount-a"), WithHandleKeepaliveInterval(0), WithStrictMode(session))
	t.Cleanup(adapter.Close)
	raw := NewRawFileSystem(adapter)
	_, status := raw.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 7, Unique: 42}, Fh: 9}, []byte("15"))
	require.Equal(t, fuse.Status(syscall.EIO), status)
	flush := &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 10}
	require.Equal(t, fuse.Status(syscall.EIO), raw.Flush(nil, flush))
	raw.OnReplyAttempt(42, 16, fuse.Status(syscall.EIO), fuse.OK)
	adapter.processAcknowledgementBatch(t.Context())
	require.Equal(t, fuse.Status(syscall.EIO), raw.Flush(nil, flush), "reply attempt alone cannot clear an unresolved write")
	core.terminal.Store(true)
	adapter.processAcknowledgementBatch(t.Context())
	require.Eventually(t, func() bool { return raw.Flush(nil, flush) == fuse.OK }, time.Second, time.Millisecond)
	require.EqualValues(t, 1, core.writes.Load(), "recovery must not issue a new write")
}
