package fuseadapter

import (
	"context"
	"syscall"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const defaultReaddirLimit = 128

// Core is the filesystem backend surface required by the FUSE adapter.
type Core interface {
	Resolve(context.Context, uint64, []byte) (uint64, error)
	GetAttr(context.Context, uint64) (filesystem.Stat, error)
	SetAttr(context.Context, uint64, filesystem.SetAttrMask, filesystem.SetAttr) (filesystem.Stat, error)
	Open(context.Context, uint64, []byte) (uint64, error)
	Read(context.Context, uint64, uint64, uint64, uint64) ([]byte, error)
	Write(context.Context, uint64, uint64, uint64, []byte) (int, error)
	Flush(context.Context, uint64, uint64) error
	Fsync(context.Context, uint64, uint64, bool) error
	Release(context.Context, uint64, uint64, []byte) error
	RefreshOpenHandleLease(context.Context, uint64, uint64, []byte) error
	Create(context.Context, uint64, []byte, filesystem.CreateOptions) (filesystem.CreateResult, error)
	Mkdir(context.Context, uint64, []byte, filesystem.CreateOptions) (filesystem.CreateResult, error)
	Unlink(context.Context, uint64, []byte) error
	Rmdir(context.Context, uint64, []byte) error
	Rename(context.Context, uint64, []byte, uint64, []byte) error
	Readdir(context.Context, uint64, string, int) (filesystem.ReaddirResult, error)
	StatFS(context.Context, uint64) (filesystem.StatFS, error)
}

type Adapter struct {
	core         Core
	clientID     []byte
	readdirLimit int
}

type Option func(*Adapter)

var errnoMappings = []struct {
	err   error
	errno syscall.Errno
}{
	{context.Canceled, syscall.EINTR},
	{context.DeadlineExceeded, syscall.ETIMEDOUT},
	{filesystem.ErrNotFound, syscall.ENOENT},
	{filesystem.ErrExists, syscall.EEXIST},
	{filesystem.ErrNotDir, syscall.ENOTDIR},
	{filesystem.ErrIsDir, syscall.EISDIR},
	{filesystem.ErrNotEmpty, syscall.ENOTEMPTY},
	{filesystem.ErrCrossDevice, syscall.EXDEV},
	{filesystem.ErrInvalid, syscall.EINVAL},
	{filesystem.ErrUnsupported, syscall.EOPNOTSUPP},
	{store.ErrWriteConflict, syscall.EAGAIN},
}

func WithReaddirLimit(limit int) Option {
	return func(a *Adapter) {
		if limit > 0 {
			a.readdirLimit = limit
		}
	}
}

func New(core Core, clientID []byte, opts ...Option) *Adapter {
	a := &Adapter{
		core:         core,
		clientID:     append([]byte(nil), clientID...),
		readdirLimit: defaultReaddirLimit,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func Errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	for _, mapping := range errnoMappings {
		if errors.Is(err, mapping.err) {
			return mapping.errno
		}
	}
	return syscall.EIO
}

func (a *Adapter) Lookup(ctx context.Context, parent uint64, name []byte) (filesystem.Stat, syscall.Errno) {
	inode, err := a.core.Resolve(ctx, parent, name)
	if errno := Errno(err); errno != 0 {
		return filesystem.Stat{}, errno
	}
	stat, err := a.core.GetAttr(ctx, inode)
	return stat, Errno(err)
}

func (a *Adapter) GetAttr(ctx context.Context, inode uint64) (filesystem.Stat, syscall.Errno) {
	stat, err := a.core.GetAttr(ctx, inode)
	return stat, Errno(err)
}

func (a *Adapter) SetAttr(
	ctx context.Context,
	inode uint64,
	mask filesystem.SetAttrMask,
	attrs filesystem.SetAttr,
) (filesystem.Stat, syscall.Errno) {
	stat, err := a.core.SetAttr(ctx, inode, mask, attrs)
	return stat, Errno(err)
}

func (a *Adapter) Open(ctx context.Context, inode uint64) (uint64, syscall.Errno) {
	fh, err := a.core.Open(ctx, inode, a.clientID)
	return fh, Errno(err)
}

func (a *Adapter) Read(ctx context.Context, inode uint64, fh uint64, offset uint64, size uint64) ([]byte, syscall.Errno) {
	if errno := a.refreshOpenHandleLease(ctx, inode, fh); errno != 0 {
		return nil, errno
	}
	data, err := a.core.Read(ctx, inode, fh, offset, size)
	return data, Errno(err)
}

func (a *Adapter) Write(ctx context.Context, inode uint64, fh uint64, offset uint64, data []byte) (int, syscall.Errno) {
	if errno := a.refreshOpenHandleLease(ctx, inode, fh); errno != 0 {
		return 0, errno
	}
	n, err := a.core.Write(ctx, inode, fh, offset, data)
	return n, Errno(err)
}

func (a *Adapter) Flush(ctx context.Context, inode uint64, fh uint64) syscall.Errno {
	if errno := a.refreshOpenHandleLease(ctx, inode, fh); errno != 0 {
		return errno
	}
	return Errno(a.core.Flush(ctx, inode, fh))
}

func (a *Adapter) Fsync(ctx context.Context, inode uint64, fh uint64, datasync bool) syscall.Errno {
	if errno := a.refreshOpenHandleLease(ctx, inode, fh); errno != 0 {
		return errno
	}
	return Errno(a.core.Fsync(ctx, inode, fh, datasync))
}

func (a *Adapter) Release(ctx context.Context, inode uint64, fh uint64) syscall.Errno {
	return Errno(a.core.Release(ctx, inode, fh, a.clientID))
}

func (a *Adapter) Create(
	ctx context.Context,
	parent uint64,
	name []byte,
	opts filesystem.CreateOptions,
) (filesystem.CreateResult, syscall.Errno) {
	opts.ClientID = append([]byte(nil), a.clientID...)
	result, err := a.core.Create(ctx, parent, name, opts)
	return result, Errno(err)
}

func (a *Adapter) Mkdir(
	ctx context.Context,
	parent uint64,
	name []byte,
	opts filesystem.CreateOptions,
) (filesystem.CreateResult, syscall.Errno) {
	result, err := a.core.Mkdir(ctx, parent, name, opts)
	return result, Errno(err)
}

func (a *Adapter) Unlink(ctx context.Context, parent uint64, name []byte) syscall.Errno {
	return Errno(a.core.Unlink(ctx, parent, name))
}

func (a *Adapter) Rmdir(ctx context.Context, parent uint64, name []byte) syscall.Errno {
	return Errno(a.core.Rmdir(ctx, parent, name))
}

func (a *Adapter) Rename(
	ctx context.Context,
	oldParent uint64,
	oldName []byte,
	newParent uint64,
	newName []byte,
) syscall.Errno {
	return Errno(a.core.Rename(ctx, oldParent, oldName, newParent, newName))
}

func (a *Adapter) Readdir(ctx context.Context, inode uint64, cookie string) (filesystem.ReaddirResult, syscall.Errno) {
	result, err := a.core.Readdir(ctx, inode, cookie, a.readdirLimit)
	return result, Errno(err)
}

func (a *Adapter) StatFS(ctx context.Context, inode uint64) (filesystem.StatFS, syscall.Errno) {
	stat, err := a.core.StatFS(ctx, inode)
	return stat, Errno(err)
}

func (a *Adapter) refreshOpenHandleLease(ctx context.Context, inode uint64, fh uint64) syscall.Errno {
	return Errno(a.core.RefreshOpenHandleLease(ctx, inode, fh, a.clientID))
}

func (*Adapter) Link(context.Context) syscall.Errno {
	return syscall.EOPNOTSUPP
}

func (*Adapter) Symlink(context.Context) syscall.Errno {
	return syscall.EOPNOTSUPP
}

func (*Adapter) Readlink(context.Context) syscall.Errno {
	return syscall.ENOSYS
}

func (*Adapter) FileLock(context.Context) syscall.Errno {
	return syscall.EOPNOTSUPP
}
