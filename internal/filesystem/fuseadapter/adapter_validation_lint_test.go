package fuseadapter

import (
	"context"
	"errors"
	"syscall"
	"testing"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

type validationCore struct {
	*fakeCore
	ids     []filesystem.OperationID
	perform func(filesystem.OperationID, uint64, []byte) error
}

func (c *validationCore) record(id filesystem.OperationID, inode uint64, data []byte) error {
	c.ids = append(c.ids, id)
	if c.perform != nil {
		return c.perform(id, inode, data)
	}
	return nil
}

type validationUnlinkCore struct{ *validationCore }

func (c *validationUnlinkCore) UnlinkWithOperation(_ context.Context, id filesystem.OperationID, parent uint64, name []byte) error {
	return c.record(id, parent, name)
}

type validationRmdirCore struct{ *validationCore }

func (c *validationRmdirCore) RmdirWithOperation(_ context.Context, id filesystem.OperationID, parent uint64, name []byte) error {
	return c.record(id, parent, name)
}

type validationWriteCore struct{ *validationCore }

func (c *validationWriteCore) WriteWithOperation(_ context.Context, id filesystem.OperationID, inode, _ uint64, data []byte) (int, error) {
	if err := c.record(id, inode, data); err != nil {
		return 0, err
	}
	return len(data), nil
}

type validationOperation struct {
	name string
	wrap func(*validationCore) Core
	opt  func(filesystem.OperationSessionID) Option
	call func(*Adapter, context.Context) syscall.Errno
}

func validationOperations() []validationOperation {
	return []validationOperation{
		{name: "unlink", wrap: func(c *validationCore) Core { return &validationUnlinkCore{c} }, opt: WithJournaledUnlink,
			call: func(a *Adapter, ctx context.Context) syscall.Errno { return a.Unlink(ctx, 7, []byte("entry")) }},
		{name: "rmdir", wrap: func(c *validationCore) Core { return &validationRmdirCore{c} }, opt: WithJournaledRmdir,
			call: func(a *Adapter, ctx context.Context) syscall.Errno { return a.Rmdir(ctx, 7, []byte("entry")) }},
		{name: "write", wrap: func(c *validationCore) Core { return &validationWriteCore{c} }, opt: WithJournaledWrites,
			call: func(a *Adapter, ctx context.Context) syscall.Errno {
				_, errno := a.Write(ctx, 7, 9, 4, []byte("entry"))
				return errno
			}},
	}
}

func validationSession() filesystem.OperationSessionID {
	return filesystem.OperationSessionID{Filesystem: [16]byte{1}, Session: [16]byte{2}}
}

func newValidationAdapter(t *testing.T, core Core, opts ...Option) *Adapter {
	t.Helper()
	opts = append(opts, WithHandleKeepaliveInterval(0))
	a := New(core, []byte("client"), opts...)
	t.Cleanup(a.Close)
	return a
}

func TestValidationRemovalPartialCapabilities(t *testing.T) {
	for _, op := range validationOperations()[:2] {
		t.Run(op.name, func(t *testing.T) {
			core := &validationCore{fakeCore: &fakeCore{}}
			a := newValidationAdapter(t, op.wrap(core), WithJournaledUnlink(validationSession()), WithJournaledRmdir(validationSession()))
			core.perform = func(id filesystem.OperationID, parent uint64, name []byte) error {
				require.EqualValues(t, 7, parent)
				require.Equal(t, []byte("entry"), name)
				require.Equal(t, validationSession().Filesystem, id.Filesystem)
				require.Equal(t, validationSession().Session, id.Session)
				require.Equal(t, 1, a.mutations.count)
				require.NotContains(t, a.receipts[parent], id)
				return nil
			}
			require.Zero(t, op.call(a, t.Context()))
			for _, other := range validationOperations()[:2] {
				if other.name != op.name {
					require.Equal(t, syscall.EOPNOTSUPP, other.call(a, t.Context()))
				}
			}
			require.EqualValues(t, 1, a.operationSequence.Load())
			require.Zero(t, op.call(a, t.Context()))
			require.EqualValues(t, 2, a.operationSequence.Load())
			require.Len(t, core.ids, 2)
			require.Len(t, a.receipts[7], 2)
			require.Contains(t, a.receipts[7], core.ids[0])
			require.Contains(t, a.receipts[7], core.ids[1])
			require.Zero(t, a.mutations.count)
			require.Zero(t, core.unlinkParent)
			require.Zero(t, core.rmdirParent)
		})
	}
}

func TestValidationRejectsBeforeOperationID(t *testing.T) {
	session := validationSession()
	for _, op := range validationOperations() {
		for _, invalid := range []struct {
			name     string
			session  filesystem.OperationSessionID
			missing  bool
			conflict bool
		}{
			{name: "missing capability", session: session, missing: true},
			{name: "missing filesystem", session: filesystem.OperationSessionID{Session: session.Session}},
			{name: "missing session", session: filesystem.OperationSessionID{Filesystem: session.Filesystem}},
			{name: "conflicting session", session: session, conflict: true},
		} {
			t.Run(op.name+"/"+invalid.name, func(t *testing.T) {
				core := &validationCore{fakeCore: &fakeCore{}}
				var backend Core = core
				if !invalid.missing {
					backend = op.wrap(core)
				}
				opts := []Option{op.opt(invalid.session)}
				if invalid.conflict {
					opts = append(opts, op.opt(filesystem.OperationSessionID{Filesystem: session.Filesystem, Session: [16]byte{3}}))
				}
				a := newValidationAdapter(t, backend, opts...)
				require.Equal(t, syscall.EOPNOTSUPP, op.call(a, t.Context()))
				require.Zero(t, a.operationSequence.Load())
				require.Empty(t, core.ids)
				require.Empty(t, a.receipts)
				require.Zero(t, a.mutations.count)
				require.Zero(t, a.FsyncDirectory(t.Context(), 7))
				require.False(t, core.writeCalled)
				require.Zero(t, core.unlinkParent)
				require.Zero(t, core.rmdirParent)
			})
		}
	}
}

func TestValidationMutationOutcomeClassification(t *testing.T) {
	for _, op := range validationOperations() {
		for _, outcome := range []struct {
			name    string
			err     error
			unknown bool
		}{
			{name: "unknown", err: errors.Join(kv.ErrTransactionOutcomeUnknown, context.DeadlineExceeded), unknown: true},
			{name: "ordinary EIO", err: errors.New("storage unavailable")},
		} {
			t.Run(op.name+"/"+outcome.name, func(t *testing.T) {
				core := &validationCore{fakeCore: &fakeCore{}}
				a := newValidationAdapter(t, op.wrap(core), op.opt(validationSession()))
				core.perform = func(id filesystem.OperationID, inode uint64, _ []byte) error {
					require.Equal(t, 1, a.mutations.count)
					require.NotContains(t, a.receipts[inode], id)
					return outcome.err
				}
				require.Equal(t, syscall.EIO, op.call(a, t.Context()))
				require.Zero(t, a.mutations.count)
				require.Empty(t, a.receipts)
				core.perform = nil
				if outcome.unknown {
					require.Equal(t, syscall.EIO, a.FsyncDirectory(t.Context(), 7))
					require.Equal(t, syscall.EIO, op.call(a, t.Context()))
					require.False(t, core.fsyncCalled)
					require.Len(t, core.ids, 1)
					require.EqualValues(t, 1, a.operationSequence.Load())
				} else {
					require.Zero(t, a.FsyncDirectory(t.Context(), 7))
					require.True(t, core.fsyncCalled)
					require.Zero(t, op.call(a, t.Context()))
					require.Len(t, core.ids, 2)
					require.Contains(t, a.receipts[7], core.ids[1])
				}
			})
		}
	}
}

func TestValidationMutationAdmissionPrecedesOperation(t *testing.T) {
	for _, op := range validationOperations() {
		t.Run(op.name, func(t *testing.T) {
			core := &validationCore{fakeCore: &fakeCore{}}
			a := newValidationAdapter(t, op.wrap(core), op.opt(validationSession()))
			ticket, errno := a.mutations.begin(t.Context(), 7)
			require.Zero(t, errno)
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			require.Equal(t, syscall.EINTR, op.call(a, ctx))
			a.mutations.end(ticket, false)
			require.Zero(t, a.operationSequence.Load())
			require.Empty(t, core.ids)
			require.Zero(t, core.refreshInode)
			require.Zero(t, a.mutations.count)
		})
	}
}

func TestValidationEmptyWriteRefreshesBeforeCapabilityCheck(t *testing.T) {
	core := &fakeCore{}
	a := newValidationAdapter(t, core, WithJournaledWrites(filesystem.OperationSessionID{}))
	n, errno := a.Write(t.Context(), 7, 9, 4, nil)
	require.Zero(t, errno)
	require.Zero(t, n)
	require.EqualValues(t, 7, core.refreshInode)
	require.EqualValues(t, 9, core.refreshFH)
	require.Zero(t, a.operationSequence.Load())
	require.False(t, core.writeCalled)
	core.refreshErr = filesystem.ErrStaleHome
	n, errno = a.Write(t.Context(), 7, 9, 4, nil)
	require.Equal(t, syscall.EAGAIN, errno)
	require.Zero(t, n)
	require.Zero(t, a.operationSequence.Load())
	require.Zero(t, a.mutations.count)
}
