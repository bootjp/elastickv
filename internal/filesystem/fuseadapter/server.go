package fuseadapter

import (
	"context"
	"log/slog"
	"math"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/cockroachdb/errors"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const (
	defaultAttrTTL         = 0
	defaultEntryTTL        = 0
	defaultBlockSize       = 4096
	defaultMaxNameLen      = 255
	fuseBlockUnit          = 512
	rootDirHandle          = 1
	permissionModeMask     = 0o7777
	startupRecoveryTimeout = 5 * time.Minute
	filesystemName         = "elastickv"
)

type dirPosition struct {
	cookie string
	skip   int
}

type directoryHandle struct {
	mu        sync.Mutex
	positions map[uint64]dirPosition
}

type intentRecoverer interface {
	RecoverIntents(context.Context, int) (filesystem.RecoveryStats, error)
}

// RawFileSystem binds the protocol-neutral adapter to go-fuse's wire API.
// Embedding the default implementation keeps out-of-scope operations explicit:
// go-fuse returns ENOSYS unless this type overrides the operation below.
type RawFileSystem struct {
	fuse.RawFileSystem

	adapter *Adapter

	dirMu      sync.Mutex
	nextDirFH  uint64
	dirHandles map[uint64]*directoryHandle
	attrTTL    time.Duration
	entryTTL   time.Duration
	filesystem string
}

// NewRawFileSystem creates the request bridge without mounting it. Tests and
// embedders can exercise this surface directly; Mount performs the kernel bind.
func NewRawFileSystem(adapter *Adapter) *RawFileSystem {
	return &RawFileSystem{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		adapter:       adapter,
		nextDirFH:     rootDirHandle,
		dirHandles:    make(map[uint64]*directoryHandle),
		attrTTL:       defaultAttrTTL,
		entryTTL:      defaultEntryTTL,
		filesystem:    filesystemName,
	}
}

func (r *RawFileSystem) String() string {
	if r == nil || r.filesystem == "" {
		return filesystemName
	}
	return r.filesystem

}

func (r *RawFileSystem) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	stat, errno := r.adapter.Lookup(ctx, header.NodeId, []byte(name))
	if errno != 0 {
		return fuse.Status(errno)
	}
	r.fillEntry(out, stat)
	return fuse.OK
}

func (r *RawFileSystem) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	stat, errno := r.adapter.GetAttr(ctx, input.NodeId)
	if errno != 0 {
		return fuse.Status(errno)
	}
	out.Attr = fuseAttr(stat)
	out.SetTimeout(r.attrTTL)
	return fuse.OK
}

func (r *RawFileSystem) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	mask, attrs := fuseSetAttr(input)
	stat, errno := r.adapter.SetAttr(ctx, input.NodeId, mask, attrs)
	if errno != 0 {
		return fuse.Status(errno)
	}
	out.Attr = fuseAttr(stat)
	out.SetTimeout(r.attrTTL)
	return fuse.OK
}

func (r *RawFileSystem) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	if input.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return fuse.Status(syscall.EOPNOTSUPP)
	}
	ctx, done := requestContext(cancel)
	defer done()
	result, errno := r.adapter.Create(ctx, input.NodeId, []byte(name), filesystem.CreateOptions{
		Mode: input.Mode & permissionModeMask,
		UID:  input.Uid,
		GID:  input.Gid,
	})
	if errno != 0 {
		return fuse.Status(errno)
	}
	if result.FH != 0 {
		if releaseErrno := r.adapter.Release(ctx, result.Inode, result.FH); releaseErrno != 0 {
			return fuse.Status(releaseErrno)
		}
	}
	r.fillEntry(out, result.Stat)
	return fuse.OK
}

func (r *RawFileSystem) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	result, errno := r.adapter.Mkdir(ctx, input.NodeId, []byte(name), filesystem.CreateOptions{
		Mode: input.Mode & permissionModeMask,
		UID:  input.Uid,
		GID:  input.Gid,
	})
	if errno != 0 {
		return fuse.Status(errno)
	}
	r.fillEntry(out, result.Stat)
	return fuse.OK
}

func (r *RawFileSystem) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	return fuse.Status(r.adapter.Unlink(ctx, header.NodeId, []byte(name)))
}

func (r *RawFileSystem) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	return fuse.Status(r.adapter.Rmdir(ctx, header.NodeId, []byte(name)))
}

func (r *RawFileSystem) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	if input.Flags != 0 {
		return fuse.Status(syscall.EOPNOTSUPP)
	}
	ctx, done := requestContext(cancel)
	defer done()
	return fuse.Status(r.adapter.Rename(ctx, input.NodeId, []byte(oldName), input.Newdir, []byte(newName)))
}

func (r *RawFileSystem) Link(<-chan struct{}, *fuse.LinkIn, string, *fuse.EntryOut) fuse.Status {
	return fuse.Status(syscall.EOPNOTSUPP)
}

func (r *RawFileSystem) Symlink(<-chan struct{}, *fuse.InHeader, string, string, *fuse.EntryOut) fuse.Status {
	return fuse.Status(syscall.EOPNOTSUPP)
}

func (r *RawFileSystem) Readlink(<-chan struct{}, *fuse.InHeader) ([]byte, fuse.Status) {
	return nil, fuse.Status(syscall.EOPNOTSUPP)
}

func (r *RawFileSystem) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	_, errno := r.adapter.GetAttr(ctx, input.NodeId)
	return fuse.Status(errno)
}

func (r *RawFileSystem) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	result, errno := r.adapter.Create(ctx, input.NodeId, []byte(name), filesystem.CreateOptions{
		Mode: input.Mode & permissionModeMask,
		UID:  input.Uid,
		GID:  input.Gid,
	})
	if errno != 0 {
		return fuse.Status(errno)
	}
	r.fillEntry(&out.EntryOut, result.Stat)
	out.Fh = result.FH
	return fuse.OK
}

func (r *RawFileSystem) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	fh, errno := r.adapter.Open(ctx, input.NodeId)
	if errno == 0 {
		out.Fh = fh
	}
	return fuse.Status(errno)
}

func (r *RawFileSystem) Read(cancel <-chan struct{}, input *fuse.ReadIn, _ []byte) (fuse.ReadResult, fuse.Status) {
	ctx, done := requestContext(cancel)
	defer done()
	data, errno := r.adapter.Read(ctx, input.NodeId, input.Fh, input.Offset, uint64(input.Size))
	if errno != 0 {
		return nil, fuse.Status(errno)
	}
	return fuse.ReadResultData(data), fuse.OK
}

func (r *RawFileSystem) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	ctx, done := requestContext(cancel)
	defer done()
	n, errno := r.adapter.Write(ctx, input.NodeId, input.Fh, input.Offset, data)
	if errno != 0 {
		return 0, fuse.Status(errno)
	}
	if n < 0 || uint64(n) > math.MaxUint32 {
		return 0, fuse.Status(syscall.EOVERFLOW)
	}
	return uint32(n), fuse.OK //nolint:gosec // n is bounded by math.MaxUint32 immediately above.
}

func (r *RawFileSystem) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	ctx, done := requestContext(cancel)
	defer done()
	if errno := r.adapter.Release(ctx, input.NodeId, input.Fh); errno != 0 {
		slog.WarnContext(ctx, "filesystem FUSE release failed", "inode", input.NodeId, "fh", input.Fh, "errno", errno)
	}
}

func (r *RawFileSystem) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	return fuse.Status(r.adapter.Flush(ctx, input.NodeId, input.Fh))
}

func (r *RawFileSystem) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	datasync := input.FsyncFlags&1 != 0
	return fuse.Status(r.adapter.Fsync(ctx, input.NodeId, input.Fh, datasync))
}

func (r *RawFileSystem) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	stat, errno := r.adapter.GetAttr(ctx, input.NodeId)
	if errno != 0 {
		return fuse.Status(errno)
	}
	if stat.Type != filesystem.TypeDirectory {
		return fuse.Status(syscall.ENOTDIR)
	}
	out.Fh = r.allocateDirHandle()
	return fuse.OK
}

func (r *RawFileSystem) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	handle, ok := r.directoryHandle(input.Fh)
	if !ok {
		return fuse.Status(syscall.EBADF)
	}
	handle.mu.Lock()
	defer handle.mu.Unlock()
	position, ok := handle.positions[input.Offset]
	if !ok {
		return fuse.Status(syscall.EINVAL)
	}
	result, errno := r.adapter.Readdir(ctx, input.NodeId, position.cookie)
	if errno != 0 {
		return fuse.Status(errno)
	}
	if position.skip > len(result.Entries) {
		return fuse.Status(syscall.EIO)
	}
	offset := input.Offset
	for index := position.skip; index < len(result.Entries); index++ {
		entry := result.Entries[index]
		offset++
		if !out.AddDirEntry(fuse.DirEntry{
			Mode: fileTypeMode(entry.Type),
			Name: string(entry.Name),
			Ino:  entry.Inode,
			Off:  offset,
		}) {
			break
		}
		if index+1 < len(result.Entries) {
			handle.positions[offset] = dirPosition{cookie: position.cookie, skip: index + 1}
		} else {
			handle.positions[offset] = dirPosition{cookie: result.NextCookie}
		}
	}
	return fuse.OK
}

func (r *RawFileSystem) ReleaseDir(input *fuse.ReleaseIn) {
	r.dirMu.Lock()
	delete(r.dirHandles, input.Fh)
	r.dirMu.Unlock()
}

func (r *RawFileSystem) FsyncDir(<-chan struct{}, *fuse.FsyncIn) fuse.Status {
	return fuse.OK
}

func (r *RawFileSystem) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	ctx, done := requestContext(cancel)
	defer done()
	stat, errno := r.adapter.StatFS(ctx, input.NodeId)
	if errno != 0 {
		return fuse.Status(errno)
	}
	blockSize := stat.ChunkSize
	if blockSize == 0 || blockSize > math.MaxUint32 {
		blockSize = defaultBlockSize
	}
	capacity := stat.Capacity
	if capacity == 0 && stat.Free == math.MaxUint64 {
		capacity = math.MaxUint64
	}
	out.Blocks = divideRoundUp(capacity, blockSize)
	out.Bfree = divideRoundUp(stat.Free, blockSize)
	out.Bavail = out.Bfree
	out.Files = saturatingAdd(stat.Files, stat.FreeFiles)
	out.Ffree = stat.FreeFiles
	out.Bsize = uint32(blockSize)  //nolint:gosec // blockSize is bounded by math.MaxUint32 above.
	out.Frsize = uint32(blockSize) //nolint:gosec // blockSize is bounded by math.MaxUint32 above.
	out.NameLen = defaultMaxNameLen
	return fuse.OK
}

func (r *RawFileSystem) fillEntry(out *fuse.EntryOut, stat filesystem.Stat) {
	out.NodeId = stat.Inode
	out.Generation = stat.Generation
	out.Attr = fuseAttr(stat)
	out.SetEntryTimeout(r.entryTTL)
	out.SetAttrTimeout(r.attrTTL)
}

func (r *RawFileSystem) allocateDirHandle() uint64 {
	r.dirMu.Lock()
	defer r.dirMu.Unlock()
	fh := r.nextDirFH
	r.nextDirFH++
	if r.nextDirFH == 0 {
		r.nextDirFH = rootDirHandle
	}
	r.dirHandles[fh] = &directoryHandle{positions: map[uint64]dirPosition{0: {}}}
	return fh
}

func (r *RawFileSystem) directoryHandle(fh uint64) (*directoryHandle, bool) {
	r.dirMu.Lock()
	defer r.dirMu.Unlock()
	handle, ok := r.dirHandles[fh]
	return handle, ok
}

func requestContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, done := context.WithCancel(context.Background())
	if cancel == nil {
		return ctx, done
	}
	go func() {
		select {
		case <-cancel:
			done()
		case <-ctx.Done():
		}
	}()
	return ctx, done
}

func fuseSetAttr(input *fuse.SetAttrIn) (filesystem.SetAttrMask, filesystem.SetAttr) {
	var mask filesystem.SetAttrMask
	var attrs filesystem.SetAttr
	if value, ok := input.GetMode(); ok {
		mask.Mode = true
		attrs.Mode = value
	}
	if value, ok := input.GetUID(); ok {
		mask.UID = true
		attrs.UID = value
	}
	if value, ok := input.GetGID(); ok {
		mask.GID = true
		attrs.GID = value
	}
	if value, ok := input.GetSize(); ok {
		mask.Size = true
		attrs.Size = value
	}
	if value, ok := input.GetATime(); ok {
		mask.Atime = true
		attrs.AtimeNsec = value.UnixNano()
	}
	if value, ok := input.GetMTime(); ok {
		mask.Mtime = true
		attrs.MtimeNsec = value.UnixNano()
	}
	return mask, attrs
}

func fuseAttr(stat filesystem.Stat) fuse.Attr {
	atimeSec, atimeNsec := splitUnixNsec(stat.AtimeNsec)
	mtimeSec, mtimeNsec := splitUnixNsec(stat.MtimeNsec)
	ctimeSec, ctimeNsec := splitUnixNsec(stat.CtimeNsec)
	return fuse.Attr{
		Ino:       stat.Inode,
		Size:      stat.Size,
		Blocks:    divideRoundUp(stat.Size, fuseBlockUnit),
		Atime:     atimeSec,
		Mtime:     mtimeSec,
		Ctime:     ctimeSec,
		Atimensec: atimeNsec,
		Mtimensec: mtimeNsec,
		Ctimensec: ctimeNsec,
		Mode:      fileTypeMode(stat.Type) | stat.Mode&permissionModeMask,
		Nlink:     stat.Nlink,
		Owner:     fuse.Owner{Uid: stat.UID, Gid: stat.GID},
		Blksize:   defaultBlockSize,
	}
}

func fileTypeMode(typ filesystem.FileType) uint32 {
	if typ == filesystem.TypeDirectory {
		return syscall.S_IFDIR
	}
	return syscall.S_IFREG
}

func splitUnixNsec(value int64) (uint64, uint32) {
	if value <= 0 {
		return 0, 0
	}
	return uint64(value / int64(time.Second)), uint32(value % int64(time.Second)) //nolint:gosec // positive int64 components fit their unsigned destinations.
}

func divideRoundUp(value uint64, divisor uint64) uint64 {
	if value == 0 || divisor == 0 {
		return 0
	}
	return 1 + (value-1)/divisor
}

func saturatingAdd(left uint64, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

// Server owns an active kernel mount and its adapter session.
type Server struct {
	inner   *fuse.Server
	adapter *Adapter
}

// Mount attaches adapter to mountPoint using go-fuse. The returned server is
// ready for Serve; callers should call Unmount during shutdown.
func Mount(mountPoint string, adapter *Adapter, options *fuse.MountOptions) (*Server, error) {
	if adapter == nil || adapter.core == nil {
		return nil, errors.New("filesystem FUSE adapter is required")
	}
	if strings.TrimSpace(mountPoint) == "" {
		return nil, errors.New("filesystem FUSE mount point is required")
	}
	opts := defaultMountOptions(options)
	if recoverer, ok := adapter.core.(intentRecoverer); ok {
		recoveryCtx, cancel := context.WithTimeout(context.Background(), startupRecoveryTimeout)
		_, recoveryErr := recoverer.RecoverIntents(recoveryCtx, 0)
		cancel()
		if recoveryErr != nil {
			return nil, errors.Wrap(recoveryErr, "recover filesystem intents before FUSE mount")
		}
	}
	inner, err := fuse.NewServer(NewRawFileSystem(adapter), mountPoint, opts)
	if err != nil {
		return nil, errors.Wrap(err, "mount filesystem FUSE server")
	}
	return &Server{inner: inner, adapter: adapter}, nil
}

func defaultMountOptions(options *fuse.MountOptions) *fuse.MountOptions {
	var opts fuse.MountOptions
	if options != nil {
		opts = *options
		opts.Options = append([]string(nil), options.Options...)
	}
	if opts.FsName == "" {
		opts.FsName = filesystemName
	}
	if opts.Name == "" {
		opts.Name = filesystemName
	}
	opts.DisableReadDirPlus = true
	opts.DisableXAttrs = true
	if !containsMountOption(opts.Options, "default_permissions") {
		opts.Options = append(opts.Options, "default_permissions")
	}
	return &opts
}

func containsMountOption(options []string, want string) bool {
	for _, option := range options {
		if option == want {
			return true
		}
	}
	return false
}

func (s *Server) Serve() {
	if s != nil && s.inner != nil {
		s.inner.Serve()
	}
}

func (s *Server) WaitMount() error {
	if s == nil || s.inner == nil {
		return nil
	}
	return errors.WithStack(s.inner.WaitMount())
}

func (s *Server) Wait() {
	if s != nil && s.inner != nil {
		s.inner.Wait()
	}
}

func (s *Server) Unmount() error {
	if s == nil {
		return nil
	}
	if s.adapter != nil {
		s.adapter.Close()
	}
	if s.inner == nil {
		return nil
	}
	return errors.WithStack(s.inner.Unmount())
}
