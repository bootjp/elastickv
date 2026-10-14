package fuseadapter

import (
	"context"
	"errors"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func newRawFileSystem(t *testing.T, core Core) (*RawFileSystem, *Adapter) {
	t.Helper()
	adapter := New(core, []byte("mount-a"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	return NewRawFileSystem(adapter), adapter
}

func TestRawFileSystemStringReportsFilesystemName(t *testing.T) {
	raw, _ := newRawFileSystem(t, &fakeCore{})
	require.Equal(t, filesystemName, raw.String())
	require.Equal(t, filesystemName, (*RawFileSystem)(nil).String())
	raw.filesystem = ""
	require.Equal(t, filesystemName, raw.String())
}

func TestRawFileSystemSetAttrTranslatesValidMask(t *testing.T) {
	core := &fakeCore{setAttrStat: filesystem.Stat{
		Inode: 7,
		Type:  filesystem.TypeFile,
		Mode:  0o600 | syscall.S_IFREG,
		Size:  5,
	}}
	raw, _ := newRawFileSystem(t, core)

	input := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{
		InHeader:  fuse.InHeader{NodeId: 7},
		Valid:     fuse.FATTR_MODE | fuse.FATTR_UID | fuse.FATTR_GID | fuse.FATTR_SIZE | fuse.FATTR_ATIME | fuse.FATTR_MTIME,
		Mode:      syscall.S_IFREG | 0o600,
		Owner:     fuse.Owner{Uid: 1000, Gid: 1001},
		Size:      5,
		Atime:     1,
		Atimensec: 2,
		Mtime:     3,
		Mtimensec: 4,
	}}
	var out fuse.AttrOut
	status := raw.SetAttr(nil, input, &out)
	require.Equal(t, fuse.OK, status)
	require.EqualValues(t, 7, core.setAttrInode)
	require.Equal(t, filesystem.SetAttrMask{Mode: true, UID: true, GID: true, Size: true, Atime: true, Mtime: true}, core.setAttrMask)
	require.EqualValues(t, 0o600, core.setAttrAttrs.Mode)
	require.EqualValues(t, 1000, core.setAttrAttrs.UID)
	require.EqualValues(t, 1001, core.setAttrAttrs.GID)
	require.EqualValues(t, 5, core.setAttrAttrs.Size)
	require.EqualValues(t, time.Second.Nanoseconds()+2, core.setAttrAttrs.AtimeNsec)
	require.EqualValues(t, 3*time.Second.Nanoseconds()+4, core.setAttrAttrs.MtimeNsec)
	require.EqualValues(t, 7, out.Ino)
	require.EqualValues(t, 5, out.Size)
	require.Equal(t, uint32(syscall.S_IFREG|0o600), out.Mode)
}

func TestRawFileSystemSetAttrSizeOnlyLeavesOtherMaskBitsClear(t *testing.T) {
	core := &fakeCore{}
	raw, _ := newRawFileSystem(t, core)

	input := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{
		InHeader: fuse.InHeader{NodeId: 7},
		Valid:    fuse.FATTR_SIZE | fuse.FATTR_FH,
		Fh:       9,
		Size:     0,
		Mode:     0o777,
	}}
	var out fuse.AttrOut
	require.Equal(t, fuse.OK, raw.SetAttr(nil, input, &out))
	require.Equal(t, filesystem.SetAttrMask{Size: true}, core.setAttrMask)
	require.Zero(t, core.setAttrAttrs.Mode)

	core.setAttrErr = filesystem.ErrNotFound
	require.Equal(t, fuse.Status(syscall.ENOENT), raw.SetAttr(nil, input, &out))
}

func TestRawFileSystemGetAttrAndAccessMapErrors(t *testing.T) {
	core := &fakeCore{stat: filesystem.Stat{Inode: 7, Type: filesystem.TypeDirectory, Mode: 0o755}}
	raw, _ := newRawFileSystem(t, core)

	var out fuse.AttrOut
	require.Equal(t, fuse.OK, raw.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 7}}, &out))
	require.Equal(t, uint32(syscall.S_IFDIR|0o755), out.Mode)
	require.Equal(t, fuse.OK, raw.Access(nil, &fuse.AccessIn{InHeader: fuse.InHeader{NodeId: 7}}))

	core.getAttrErr = filesystem.ErrNotFound
	require.Equal(t, fuse.Status(syscall.ENOENT), raw.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 7}}, &out))
	require.Equal(t, fuse.Status(syscall.ENOENT), raw.Access(nil, &fuse.AccessIn{InHeader: fuse.InHeader{NodeId: 7}}))
}

func TestRawFileSystemLookupMapsErrors(t *testing.T) {
	core := &fakeCore{resolveErr: filesystem.ErrNotFound}
	raw, _ := newRawFileSystem(t, core)

	var out fuse.EntryOut
	status := raw.Lookup(nil, &fuse.InHeader{NodeId: filesystem.RootInode}, "missing", &out)
	require.Equal(t, fuse.Status(syscall.ENOENT), status)
	require.Zero(t, out.NodeId)
}

func TestRawFileSystemMkdirMasksModeAndForwardsCaller(t *testing.T) {
	core := &fakeCore{mkdirResult: filesystem.CreateResult{
		Inode: 11,
		Stat:  filesystem.Stat{Inode: 11, Generation: 1, Type: filesystem.TypeDirectory, Mode: 0o750, Nlink: 2},
	}}
	raw, _ := newRawFileSystem(t, core)

	var out fuse.EntryOut
	status := raw.Mkdir(nil, &fuse.MkdirIn{
		InHeader: fuse.InHeader{NodeId: filesystem.RootInode, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1001}}},
		Mode:     syscall.S_IFDIR | 0o750,
	}, "dir", &out)
	require.Equal(t, fuse.OK, status)
	require.EqualValues(t, 11, out.NodeId)
	require.Equal(t, uint32(syscall.S_IFDIR|0o750), out.Mode)
	require.EqualValues(t, filesystem.RootInode, core.mkdirParent)
	require.Equal(t, []byte("dir"), core.mkdirName)
	require.EqualValues(t, 0o750, core.mkdirOpts.Mode)
	require.EqualValues(t, 1000, core.mkdirOpts.UID)
	require.EqualValues(t, 1001, core.mkdirOpts.GID)

	core.mkdirErr = filesystem.ErrExists
	require.Equal(t, fuse.Status(syscall.EEXIST), raw.Mkdir(nil, &fuse.MkdirIn{
		InHeader: fuse.InHeader{NodeId: filesystem.RootInode},
	}, "dir", &out))
}

func TestRawFileSystemMknodRejectsNonRegularNodes(t *testing.T) {
	core := &fakeCore{}
	raw, _ := newRawFileSystem(t, core)

	var out fuse.EntryOut
	for _, mode := range []uint32{syscall.S_IFCHR, syscall.S_IFBLK, syscall.S_IFIFO, syscall.S_IFSOCK} {
		status := raw.Mknod(nil, &fuse.MknodIn{
			InHeader: fuse.InHeader{NodeId: filesystem.RootInode},
			Mode:     mode | 0o600,
		}, "node", &out)
		require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), status)
	}
	require.Zero(t, core.createOpts.Mode)

	core.createErr = filesystem.ErrExists
	status := raw.Mknod(nil, &fuse.MknodIn{
		InHeader: fuse.InHeader{NodeId: filesystem.RootInode},
		Mode:     syscall.S_IFREG | 0o600,
	}, "node", &out)
	require.Equal(t, fuse.Status(syscall.EEXIST), status)
}

func TestRawFileSystemCreateMapsErrors(t *testing.T) {
	core := &fakeCore{createErr: filesystem.ErrStaleHome}
	raw, adapter := newRawFileSystem(t, core)

	var out fuse.CreateOut
	status := raw.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}}, "file", &out)
	require.Equal(t, fuse.Status(syscall.EAGAIN), status)
	require.Zero(t, out.Fh)
	require.Empty(t, adapter.openHandleSnapshot())
}

func TestRawFileSystemUnlinkAndRmdirForwardNames(t *testing.T) {
	core := &fakeCore{}
	raw, _ := newRawFileSystem(t, core)

	require.Equal(t, fuse.OK, raw.Unlink(nil, &fuse.InHeader{NodeId: 5}, "file"))
	require.EqualValues(t, 5, core.unlinkParent)
	require.Equal(t, []byte("file"), core.unlinkName)

	require.Equal(t, fuse.OK, raw.Rmdir(nil, &fuse.InHeader{NodeId: 5}, "dir"))
	require.EqualValues(t, 5, core.rmdirParent)
	require.Equal(t, []byte("dir"), core.rmdirName)

	core.unlinkErr = filesystem.ErrIsDir
	core.rmdirErr = filesystem.ErrNotEmpty
	require.Equal(t, fuse.Status(syscall.EISDIR), raw.Unlink(nil, &fuse.InHeader{NodeId: 5}, "file"))
	require.Equal(t, fuse.Status(syscall.ENOTEMPTY), raw.Rmdir(nil, &fuse.InHeader{NodeId: 5}, "dir"))
}

func TestRawFileSystemRenameForwardsAndRejectsFlags(t *testing.T) {
	core := &fakeCore{}
	raw, _ := newRawFileSystem(t, core)

	status := raw.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: 5}, Newdir: 6}, "old", "new")
	require.Equal(t, fuse.OK, status)
	require.Equal(t, renameCall{oldParent: 5, oldName: []byte("old"), newParent: 6, newName: []byte("new")}, core.rename)

	core.rename = renameCall{}
	status = raw.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: 5}, Newdir: 6, Flags: 1}, "old", "new")
	require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), status)
	require.Equal(t, renameCall{}, core.rename)

	core.renameErr = filesystem.ErrCrossDevice
	status = raw.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: 5}, Newdir: 6}, "old", "new")
	require.Equal(t, fuse.Status(syscall.EXDEV), status)
}

func TestRawFileSystemUnsupportedLinkOperations(t *testing.T) {
	raw, _ := newRawFileSystem(t, &fakeCore{})

	var out fuse.EntryOut
	require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), raw.Link(nil, &fuse.LinkIn{}, "link", &out))
	require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), raw.Symlink(nil, &fuse.InHeader{}, "target", "link", &out))
	data, status := raw.Readlink(nil, &fuse.InHeader{})
	require.Nil(t, data)
	require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), status)
}

func TestRawFileSystemOpenTracksHandleAndMapsErrors(t *testing.T) {
	core := &fakeCore{openFH: 9}
	raw, adapter := newRawFileSystem(t, core)

	var out fuse.OpenOut
	require.Equal(t, fuse.OK, raw.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 7}}, &out))
	require.EqualValues(t, 9, out.Fh)
	require.Len(t, adapter.openHandleSnapshot(), 1)

	core.openErr = filesystem.ErrIsDir
	out = fuse.OpenOut{}
	require.Equal(t, fuse.Status(syscall.EISDIR), raw.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 7}}, &out))
	require.Zero(t, out.Fh)
}

func TestRawFileSystemReadMapsErrors(t *testing.T) {
	core := &fakeCore{readErr: filesystem.ErrNotFound}
	raw, _ := newRawFileSystem(t, core)

	result, status := raw.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9, Offset: 3, Size: 8}, nil)
	require.Nil(t, result)
	require.Equal(t, fuse.Status(syscall.ENOENT), status)
	require.EqualValues(t, 3, core.readOffset)
	require.EqualValues(t, 8, core.readSize)
}

func TestRawFileSystemWriteReturnsByteCount(t *testing.T) {
	core := &fakeCore{}
	raw, _ := newRawFileSystem(t, core)

	n, status := raw.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9, Offset: 4}, []byte("data"))
	require.Equal(t, fuse.OK, status)
	require.EqualValues(t, 4, n)
	require.EqualValues(t, 7, core.writeInode)
	require.EqualValues(t, 9, core.writeFH)
	require.EqualValues(t, 4, core.writeOffset)
	require.Equal(t, []byte("data"), core.writeData)
	require.EqualValues(t, 9, core.refreshFH)
}

func TestRawFileSystemWriteMapsErrorsAndOverflow(t *testing.T) {
	core := &fakeCore{writeErr: filesystem.ErrStaleHome}
	raw, _ := newRawFileSystem(t, core)

	n, status := raw.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9}, []byte("data"))
	require.Zero(t, n)
	require.Equal(t, fuse.Status(syscall.EAGAIN), status)

	core.writeErr = nil
	core.writeN = -1
	n, status = raw.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9}, []byte("data"))
	require.Zero(t, n)
	require.Equal(t, fuse.Status(syscall.EOVERFLOW), status)

	if math.MaxInt > math.MaxUint32 {
		core.writeN = int(math.MaxUint32) + 1
		n, status = raw.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9}, []byte("data"))
		require.Zero(t, n)
		require.Equal(t, fuse.Status(syscall.EOVERFLOW), status)
	}
}

func TestRawFileSystemFlushAndFsyncForwardHandles(t *testing.T) {
	core := &fakeCore{}
	raw, _ := newRawFileSystem(t, core)

	require.Equal(t, fuse.OK, raw.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9}))
	require.True(t, core.flushCalled)
	require.EqualValues(t, 9, core.flushFH)

	require.Equal(t, fuse.OK, raw.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9}))
	require.True(t, core.fsyncCalled)
	require.False(t, core.fsyncDatasync)

	require.Equal(t, fuse.OK, raw.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9, FsyncFlags: 1}))
	require.True(t, core.fsyncDatasync)

	require.Equal(t, fuse.OK, raw.FsyncDir(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}}))

	core.flushErr = errors.New("boom")
	core.fsyncErr = filesystem.ErrNotFound
	require.Equal(t, fuse.Status(syscall.EIO), raw.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9}))
	require.Equal(t, fuse.Status(syscall.ENOENT), raw.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9}))
}

func TestRawFileSystemReleaseUntracksHandleAndToleratesErrors(t *testing.T) {
	core := &fakeCore{openFH: 9}
	raw, adapter := newRawFileSystem(t, core)

	var out fuse.OpenOut
	require.Equal(t, fuse.OK, raw.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 7}}, &out))
	require.Len(t, adapter.openHandleSnapshot(), 1)

	core.releaseErr = filesystem.ErrNotFound
	raw.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 7}, Fh: 9})
	require.EqualValues(t, 7, core.releaseInode)
	require.EqualValues(t, 9, core.releaseFH)
	require.Equal(t, []byte("mount-a"), core.releaseClientID)
	require.Empty(t, adapter.openHandleSnapshot())
}

func TestRawFileSystemOpenDirRejectsNonDirectories(t *testing.T) {
	core := &fakeCore{stat: filesystem.Stat{Inode: 7, Type: filesystem.TypeFile}}
	raw, _ := newRawFileSystem(t, core)

	var out fuse.OpenOut
	require.Equal(t, fuse.Status(syscall.ENOTDIR), raw.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 7}}, &out))
	require.Zero(t, out.Fh)
	require.Empty(t, raw.dirHandles)

	core.getAttrErr = filesystem.ErrNotFound
	require.Equal(t, fuse.Status(syscall.ENOENT), raw.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 7}}, &out))
}

func TestRawFileSystemReadDirRejectsUnknownOffsets(t *testing.T) {
	core := &fakeCore{stat: filesystem.Stat{Inode: filesystem.RootInode, Type: filesystem.TypeDirectory}}
	raw, _ := newRawFileSystem(t, core)

	var openOut fuse.OpenOut
	require.Equal(t, fuse.OK, raw.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}}, &openOut))

	status := raw.ReadDir(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: filesystem.RootInode},
		Fh:       openOut.Fh,
		Offset:   99,
	}, fuse.NewDirEntryList(make([]byte, 256), 99))
	require.Equal(t, fuse.Status(syscall.EINVAL), status)
}

func TestRawFileSystemReadDirMapsBackendErrors(t *testing.T) {
	core := &fakeCore{
		stat:       filesystem.Stat{Inode: filesystem.RootInode, Type: filesystem.TypeDirectory},
		readdirErr: filesystem.ErrNotDir,
	}
	raw, _ := newRawFileSystem(t, core)

	var openOut fuse.OpenOut
	require.Equal(t, fuse.OK, raw.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}}, &openOut))

	status := raw.ReadDir(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: filesystem.RootInode},
		Fh:       openOut.Fh,
	}, fuse.NewDirEntryList(make([]byte, 256), 0))
	require.Equal(t, fuse.Status(syscall.ENOTDIR), status)
}

func TestRawFileSystemReadDirEmptyDirectoryBecomesTerminal(t *testing.T) {
	core := &fakeCore{stat: filesystem.Stat{Inode: filesystem.RootInode, Type: filesystem.TypeDirectory}}
	raw, _ := newRawFileSystem(t, core)

	var openOut fuse.OpenOut
	require.Equal(t, fuse.OK, raw.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}}, &openOut))

	list := fuse.NewDirEntryList(make([]byte, 256), 0)
	status := raw.ReadDir(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}, Fh: openOut.Fh}, list)
	require.Equal(t, fuse.OK, status)
	require.Zero(t, list.Offset)

	core.readdirResult = filesystem.ReaddirResult{Entries: []filesystem.Dirent{{Name: []byte("late"), Inode: 2}}}
	status = raw.ReadDir(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}, Fh: openOut.Fh}, list)
	require.Equal(t, fuse.OK, status)
	require.Zero(t, list.Offset)
}

func TestRawFileSystemReadDirResumesMidPageWhenBufferFills(t *testing.T) {
	core := &pagedReaddirCore{fakeCore: &fakeCore{
		stat: filesystem.Stat{Inode: filesystem.RootInode, Type: filesystem.TypeDirectory},
	}}
	raw, _ := newRawFileSystem(t, core)

	var openOut fuse.OpenOut
	require.Equal(t, fuse.OK, raw.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}}, &openOut))

	var names []string
	var offset uint64
	for range 10 {
		list := fuse.NewDirEntryList(make([]byte, 40), offset)
		status := raw.ReadDir(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: filesystem.RootInode},
			Fh:       openOut.Fh,
			Offset:   offset,
		}, list)
		require.Equal(t, fuse.OK, status)
		if list.Offset == offset {
			break
		}
		offset = list.Offset
		names = append(names, "entry")
	}
	require.Len(t, names, 3)
}

func TestAddDirEntriesRejectsSkipBeyondPage(t *testing.T) {
	handle := &directoryHandle{positions: map[uint64]dirPosition{}}
	status := addDirEntries(handle, 0, dirPosition{skip: 2}, filesystem.ReaddirResult{
		Entries: []filesystem.Dirent{{Name: []byte("a"), Inode: 2}},
	}, fuse.NewDirEntryList(make([]byte, 256), 0))
	require.Equal(t, fuse.Status(syscall.EIO), status)
}

func TestAllocateDirHandleSkipsZeroOnWrap(t *testing.T) {
	raw, _ := newRawFileSystem(t, &fakeCore{})
	raw.nextDirFH = math.MaxUint64

	fh := raw.allocateDirHandle()
	require.Equal(t, uint64(math.MaxUint64), fh)
	require.Equal(t, uint64(rootDirHandle), raw.nextDirFH)

	fh = raw.allocateDirHandle()
	require.Equal(t, uint64(rootDirHandle), fh)
}

func TestRequestContextCancelsWhenKernelInterrupts(t *testing.T) {
	cancel := make(chan struct{})
	ctx, done := requestContext(cancel)
	defer done()
	require.NoError(t, ctx.Err())

	close(cancel)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("request context did not observe kernel interrupt")
	}
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestRequestContextWithoutCancelChannelEndsOnDone(t *testing.T) {
	ctx, done := requestContext(nil)
	require.NoError(t, ctx.Err())
	done()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestRawFileSystemPropagatesKernelInterruptToCore(t *testing.T) {
	core := &blockingCore{fakeCore: &fakeCore{}, started: make(chan struct{})}
	raw, _ := newRawFileSystem(t, core)

	cancel := make(chan struct{})
	result := make(chan fuse.Status, 1)
	go func() {
		var out fuse.AttrOut
		result <- raw.GetAttr(cancel, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 7}}, &out)
	}()
	<-core.started
	close(cancel)
	select {
	case status := <-result:
		require.Equal(t, fuse.Status(syscall.EINTR), status)
	case <-time.After(time.Second):
		t.Fatal("GetAttr did not return after kernel interrupt")
	}
}

func TestFuseAttrClampsNegativeTimestamps(t *testing.T) {
	attr := fuseAttr(filesystem.Stat{
		Inode:     7,
		Type:      filesystem.TypeFile,
		Mode:      0o4755 | syscall.S_IFREG,
		Size:      1,
		AtimeNsec: -1,
		MtimeNsec: 0,
		CtimeNsec: 1,
	})
	require.Zero(t, attr.Atime)
	require.Zero(t, attr.Atimensec)
	require.Zero(t, attr.Mtime)
	require.Zero(t, attr.Ctime)
	require.EqualValues(t, 1, attr.Ctimensec)
	require.EqualValues(t, 1, attr.Blocks)
	require.Equal(t, uint32(syscall.S_IFREG|0o4755), attr.Mode)
	require.EqualValues(t, defaultBlockSize, attr.Blksize)
}

func TestStatFsFallsBackToDefaultBlockSize(t *testing.T) {
	core := &fakeCore{statFS: filesystem.StatFS{ChunkSize: math.MaxUint32 + 1, Capacity: 8192, Free: 4096}}
	raw, _ := newRawFileSystem(t, core)

	var out fuse.StatfsOut
	require.Equal(t, fuse.OK, raw.StatFs(nil, &fuse.InHeader{NodeId: filesystem.RootInode}, &out))
	require.EqualValues(t, defaultBlockSize, out.Bsize)
	require.EqualValues(t, defaultBlockSize, out.Frsize)
	require.EqualValues(t, 2, out.Blocks)
	require.EqualValues(t, 1, out.Bfree)
	require.EqualValues(t, 1, out.Bavail)
	require.EqualValues(t, defaultMaxNameLen, out.NameLen)

	core.statFSErr = filesystem.ErrNotFound
	require.Equal(t, fuse.Status(syscall.ENOENT), raw.StatFs(nil, &fuse.InHeader{NodeId: filesystem.RootInode}, &out))
}

func TestDivideRoundUpAndSaturatingAdd(t *testing.T) {
	require.Zero(t, divideRoundUp(0, 4))
	require.Zero(t, divideRoundUp(4, 0))
	require.EqualValues(t, 1, divideRoundUp(1, 4))
	require.EqualValues(t, 1, divideRoundUp(4, 4))
	require.EqualValues(t, 2, divideRoundUp(5, 4))
	require.Equal(t, uint64(math.MaxUint64), saturatingAdd(math.MaxUint64, 1))
	require.EqualValues(t, 3, saturatingAdd(1, 2))
}

func TestMountRejectsInvalidInputs(t *testing.T) {
	_, err := Mount("/nonexistent", nil, nil)
	require.ErrorContains(t, err, "adapter is required")

	_, err = Mount("/nonexistent", &Adapter{}, nil)
	require.ErrorContains(t, err, "adapter is required")

	adapter := New(&fakeCore{}, []byte("mount-a"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	_, err = Mount("   ", adapter, nil)
	require.ErrorContains(t, err, "mount point is required")
}

type recoveringCore struct {
	*fakeCore
	stats []filesystem.RecoveryStats
	err   error
}

func (c *recoveringCore) RecoverIntents(context.Context, int) (filesystem.RecoveryStats, error) {
	if c.err != nil {
		return filesystem.RecoveryStats{}, c.err
	}
	if len(c.stats) == 0 {
		return filesystem.RecoveryStats{}, nil
	}
	stats := c.stats[0]
	c.stats = c.stats[1:]
	return stats, nil
}

func TestMountFailsWhenStartupRecoveryFails(t *testing.T) {
	core := &recoveringCore{fakeCore: &fakeCore{}, err: errors.New("recovery boom")}
	adapter := New(core, []byte("mount-a"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)

	_, err := Mount(t.TempDir(), adapter, nil)
	require.ErrorContains(t, err, "recover filesystem intents before FUSE mount")
	require.ErrorContains(t, err, "recovery boom")
}

func TestRecoverIntentsBeforeMountPropagatesErrors(t *testing.T) {
	core := &recoveringCore{fakeCore: &fakeCore{}, err: errors.New("recovery boom")}
	err := recoverIntentsBeforeMount(context.Background(), core)
	require.ErrorContains(t, err, "drain filesystem recovery batch")
}

func TestDefaultMountOptionsKeepsExistingPermissionOption(t *testing.T) {
	opts := defaultMountOptions(&fuse.MountOptions{
		FsName:  "custom",
		Name:    "custom-name",
		Options: []string{"default_permissions", "ro"},
	})
	require.Equal(t, []string{"default_permissions", "ro"}, opts.Options)
	require.Equal(t, "custom", opts.FsName)
	require.Equal(t, "custom-name", opts.Name)

	opts = defaultMountOptions(nil)
	require.Equal(t, []string{"default_permissions"}, opts.Options)
	require.Equal(t, filesystemName, opts.FsName)
}

func TestServerMethodsAreNilSafe(t *testing.T) {
	var server *Server
	server.Serve()
	server.Wait()
	require.NoError(t, server.WaitMount())
	require.NoError(t, server.Unmount())

	adapter := New(&fakeCore{}, []byte("mount-a"), WithHandleKeepaliveInterval(time.Hour))
	server = &Server{adapter: adapter}
	server.Serve()
	server.Wait()
	require.NoError(t, server.WaitMount())
	require.NoError(t, server.Unmount())
	require.NotNil(t, adapter.keepaliveCancel)
}

type blockingCore struct {
	*fakeCore
	started chan struct{}
}

func (c *blockingCore) GetAttr(ctx context.Context, _ uint64) (filesystem.Stat, error) {
	close(c.started)
	<-ctx.Done()
	return filesystem.Stat{}, ctx.Err()
}
