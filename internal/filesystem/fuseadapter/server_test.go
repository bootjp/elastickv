package fuseadapter

import (
	"context"
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestRawFileSystemLookupMapsStableInodeAndAttributes(t *testing.T) {
	core := &fakeCore{
		resolveInode: 42,
		stat: filesystem.Stat{
			Inode:      42,
			Generation: 1,
			Type:       filesystem.TypeFile,
			Mode:       0o640,
			UID:        1000,
			GID:        1001,
			Size:       513,
			Nlink:      1,
			AtimeNsec:  time.Second.Nanoseconds() + 2,
			MtimeNsec:  2*time.Second.Nanoseconds() + 3,
			CtimeNsec:  3*time.Second.Nanoseconds() + 4,
		},
	}
	adapter := New(core, []byte("mount-a"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	raw := NewRawFileSystem(adapter)

	var out fuse.EntryOut
	status := raw.Lookup(nil, &fuse.InHeader{NodeId: filesystem.RootInode}, "file", &out)
	require.Equal(t, fuse.OK, status)
	require.EqualValues(t, 42, out.NodeId)
	require.EqualValues(t, 1, out.Generation)
	require.EqualValues(t, 42, out.Ino)
	require.Equal(t, uint32(syscall.S_IFREG|0o640), out.Mode)
	require.EqualValues(t, 2, out.Blocks)
	require.EqualValues(t, 1000, out.Uid)
	require.EqualValues(t, 1001, out.Gid)
	require.EqualValues(t, 1, out.Atime)
	require.EqualValues(t, 2, out.Atimensec)
	require.EqualValues(t, filesystem.RootInode, core.resolveParent)
	require.Equal(t, []byte("file"), core.resolveName)
}

func TestRawFileSystemCreateAndReadUseFuseHandles(t *testing.T) {
	core := &fakeCore{
		createResult: filesystem.CreateResult{
			Inode: 7,
			FH:    9,
			Stat: filesystem.Stat{
				Inode:      7,
				Generation: 1,
				Type:       filesystem.TypeFile,
				Mode:       0o600,
				Nlink:      1,
			},
		},
		readData: []byte("payload"),
	}
	adapter := New(core, []byte("mount-a"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	raw := NewRawFileSystem(adapter)

	var createOut fuse.CreateOut
	status := raw.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: filesystem.RootInode, Caller: fuse.Caller{Owner: fuse.Owner{Uid: 1000, Gid: 1001}}},
		Mode:     0o600,
	}, "file", &createOut)
	require.Equal(t, fuse.OK, status)
	require.EqualValues(t, 7, createOut.NodeId)
	require.EqualValues(t, 9, createOut.Fh)
	require.EqualValues(t, 1000, core.createOpts.UID)
	require.EqualValues(t, 1001, core.createOpts.GID)

	result, status := raw.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 7},
		Fh:       9,
		Size:     64,
	}, nil)
	require.Equal(t, fuse.OK, status)
	data, status := result.Bytes(make([]byte, 64))
	require.Equal(t, fuse.OK, status)
	require.Equal(t, []byte("payload"), data)
}

func TestRawFileSystemReadDirResumesWithinBackendPage(t *testing.T) {
	core := &pagedReaddirCore{fakeCore: &fakeCore{
		stat: filesystem.Stat{Inode: filesystem.RootInode, Type: filesystem.TypeDirectory},
	}}
	adapter := New(core, []byte("mount-a"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	raw := NewRawFileSystem(adapter)

	var openOut fuse.OpenOut
	status := raw.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}}, &openOut)
	require.Equal(t, fuse.OK, status)
	require.NotZero(t, openOut.Fh)

	first := fuse.NewDirEntryList(make([]byte, 40), 0)
	status = raw.ReadDir(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: filesystem.RootInode},
		Fh:       openOut.Fh,
		Offset:   0,
	}, first)
	require.Equal(t, fuse.OK, status)
	require.EqualValues(t, 1, first.Offset)
	require.Equal(t, []string{""}, core.cookies)

	second := fuse.NewDirEntryList(make([]byte, 40), first.Offset)
	status = raw.ReadDir(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: filesystem.RootInode},
		Fh:       openOut.Fh,
		Offset:   first.Offset,
	}, second)
	require.Equal(t, fuse.OK, status)
	require.EqualValues(t, 2, second.Offset)
	require.Equal(t, []string{"", ""}, core.cookies)

	third := fuse.NewDirEntryList(make([]byte, 40), second.Offset)
	status = raw.ReadDir(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: filesystem.RootInode},
		Fh:       openOut.Fh,
		Offset:   second.Offset,
	}, third)
	require.Equal(t, fuse.OK, status)
	require.EqualValues(t, 3, third.Offset)
	require.Equal(t, []string{"", "", "next"}, core.cookies)

	raw.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: filesystem.RootInode}, Fh: openOut.Fh})
	status = raw.ReadDir(nil, &fuse.ReadIn{Fh: openOut.Fh}, fuse.NewDirEntryList(make([]byte, 40), 0))
	require.Equal(t, fuse.Status(syscall.EBADF), status)
}

func TestDefaultMountOptionsEnableKernelPermissionChecks(t *testing.T) {
	input := &fuse.MountOptions{Options: []string{"ro"}}
	opts := defaultMountOptions(input)

	require.Equal(t, []string{"ro"}, input.Options)
	require.Equal(t, []string{"ro", "default_permissions"}, opts.Options)
	require.Equal(t, "elastickv", opts.FsName)
	require.Equal(t, "elastickv", opts.Name)
	require.True(t, opts.DisableReadDirPlus)
	require.True(t, opts.DisableXAttrs)
}

func TestRawFileSystemStatFsMapsCapacityAndTotalInodes(t *testing.T) {
	core := &fakeCore{statFS: filesystem.StatFS{
		ChunkSize: 4,
		Files:     2,
		FreeFiles: 8,
		Capacity:  16,
		Free:      11,
	}}
	adapter := New(core, []byte("mount-a"), WithHandleKeepaliveInterval(0))
	t.Cleanup(adapter.Close)
	raw := NewRawFileSystem(adapter)

	var out fuse.StatfsOut
	status := raw.StatFs(nil, &fuse.InHeader{NodeId: filesystem.RootInode}, &out)
	require.Equal(t, fuse.OK, status)
	require.EqualValues(t, 4, out.Blocks)
	require.EqualValues(t, 3, out.Bfree)
	require.EqualValues(t, 10, out.Files)
	require.EqualValues(t, 8, out.Ffree)

	core.statFS = filesystem.StatFS{ChunkSize: 4, Files: 2, FreeFiles: math.MaxUint64, Free: math.MaxUint64}
	status = raw.StatFs(nil, &fuse.InHeader{NodeId: filesystem.RootInode}, &out)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint64(math.MaxUint64), out.Files)
	require.Greater(t, out.Blocks, uint64(0))
}

type pagedReaddirCore struct {
	*fakeCore
	cookies []string
}

func (c *pagedReaddirCore) Readdir(
	_ context.Context,
	_ uint64,
	cookie string,
	_ int,
) (filesystem.ReaddirResult, error) {
	c.cookies = append(c.cookies, cookie)
	if cookie == "" {
		return filesystem.ReaddirResult{
			Entries: []filesystem.Dirent{
				{Name: []byte("a"), Inode: 2, Type: filesystem.TypeFile},
				{Name: []byte("b"), Inode: 3, Type: filesystem.TypeDirectory},
			},
			NextCookie: "next",
		}, nil
	}
	if cookie == "next" {
		return filesystem.ReaddirResult{
			Entries: []filesystem.Dirent{{Name: []byte("c"), Inode: 4, Type: filesystem.TypeFile}},
		}, nil
	}
	return filesystem.ReaddirResult{}, nil
}
