package fuseadapter

import (
	"context"
	"strconv"
	"syscall"
	"testing"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestCreateCollisionChecksPermissionsBeforeOpen(t *testing.T) {
	for _, flags := range []uint32{syscall.O_RDONLY, syscall.O_WRONLY, syscall.O_RDWR, syscall.O_RDONLY | syscall.O_TRUNC} {
		t.Run(strconv.FormatUint(uint64(flags), 10), func(t *testing.T) {
			core := &fakeCore{createErr: filesystem.ErrExists, resolveInode: 7,
				stat: filesystem.Stat{Inode: 7, Type: filesystem.TypeFile, UID: 1001, GID: 1001, Mode: 0o600}, openFH: 9}
			raw, adapter := newRawFileSystem(t, core)
			input := &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode,
				Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1002, Gid: 1002}}}, Flags: flags | syscall.O_CREAT}
			var out fuse.CreateOut
			require.Equal(t, fuse.Status(syscall.EACCES), raw.Create(nil, input, "file", &out))
			require.Zero(t, core.openInode)
			require.Zero(t, core.setAttrInode)
			require.Empty(t, adapter.openHandleSnapshot())
		})
	}
}

func TestCreateCollisionTruncateClearsPrivilegeBits(t *testing.T) {
	core := &fakeCore{createErr: filesystem.ErrExists, resolveInode: 7,
		stat: filesystem.Stat{Inode: 7, Type: filesystem.TypeFile, UID: 1001, GID: 1001, Mode: 0o6755}, openFH: 9}
	raw, _ := newRawFileSystem(t, core)
	var out fuse.CreateOut
	require.Equal(t, fuse.OK, raw.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: filesystem.RootInode, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1001, Gid: 1001}}},
		Flags:    syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC,
	}, "file", &out))
	require.Equal(t, filesystem.SetAttrMask{Size: true, Mode: true}, core.setAttrMask)
	require.EqualValues(t, 0o755, core.setAttrAttrs.Mode)
}

type openDisappearingCore struct{ *disappearingCreateCore }

func (c *openDisappearingCore) Open(context.Context, uint64, []byte) (uint64, error) {
	return 0, filesystem.ErrNotFound
}

func TestCreateCollisionRetriesDisappearanceDuringOpen(t *testing.T) {
	core := &openDisappearingCore{&disappearingCreateCore{fakeCore: &fakeCore{
		resolveInode: 7, stat: filesystem.Stat{Inode: 7, Type: filesystem.TypeFile, Mode: 0o644},
		createResult: filesystem.CreateResult{Inode: 8, FH: 10, Stat: filesystem.Stat{Inode: 8, Type: filesystem.TypeFile}},
	}}}
	raw, _ := newRawFileSystem(t, core)
	var out fuse.CreateOut
	require.Equal(t, fuse.OK, raw.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}, Flags: syscall.O_CREAT}, "file", &out))
	require.Equal(t, 2, core.createCalls)
	require.EqualValues(t, 8, out.NodeId)
}

type canceledTruncateCore struct {
	*fakeCore
	releaseCanceled error
}

func (c *canceledTruncateCore) SetAttr(context.Context, uint64, filesystem.SetAttrMask, filesystem.SetAttr) (filesystem.Stat, error) {
	return filesystem.Stat{}, context.Canceled
}

func (c *canceledTruncateCore) Release(ctx context.Context, inode, fh uint64, clientID []byte) error {
	c.releaseCanceled = ctx.Err()
	return c.fakeCore.Release(ctx, inode, fh, clientID)
}

func TestCreateCollisionCleanupIgnoresCanceledRequest(t *testing.T) {
	core := &canceledTruncateCore{fakeCore: &fakeCore{openFH: 9}}
	raw, _ := newRawFileSystem(t, core)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out fuse.CreateOut
	// Exercise truncation even though the backend ignores cancellation in Open.
	require.NotEqual(t, fuse.OK, raw.openExistingForCreate(ctx, &fuse.CreateIn{Flags: syscall.O_TRUNC}, filesystem.Stat{Inode: 7, Type: filesystem.TypeFile}, &out))
	require.NoError(t, core.releaseCanceled)
	require.EqualValues(t, 9, core.releaseFH)
}
