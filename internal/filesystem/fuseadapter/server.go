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
	createRetryLimit       = 4
	startupRecoveryTimeout = 5 * time.Minute
	startupRecoveryBatch   = 256
	filesystemName         = "elastickv"
)

type dirPosition struct {
	cookie string
	skip   int
	eof    bool
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

	// requests is the parent of every request's context. cancelRequests
	// ends it, which makes in-flight backend calls return so the kernel
	// gets an answer for each request it already handed to this server.
	requests       context.Context
	cancelRequests context.CancelFunc
}

// NewRawFileSystem creates the request bridge without mounting it. Tests and
// embedders can exercise this surface directly; Mount performs the kernel bind.
func NewRawFileSystem(adapter *Adapter) *RawFileSystem {
	requests, cancelRequests := context.WithCancel(context.Background())
	return &RawFileSystem{
		RawFileSystem:  fuse.NewDefaultRawFileSystem(),
		adapter:        adapter,
		nextDirFH:      rootDirHandle,
		dirHandles:     make(map[uint64]*directoryHandle),
		attrTTL:        defaultAttrTTL,
		entryTTL:       defaultEntryTTL,
		filesystem:     filesystemName,
		requests:       requests,
		cancelRequests: cancelRequests,
	}
}

func (r *RawFileSystem) String() string {
	if r == nil || r.filesystem == "" {
		return filesystemName
	}
	return r.filesystem

}

func (r *RawFileSystem) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	ctx, done := r.requestContext(cancel)
	defer done()
	stat, errno := r.adapter.Lookup(ctx, header.NodeId, []byte(name))
	if errno != 0 {
		return fuse.Status(errno)
	}
	r.fillEntry(out, stat)
	return fuse.OK
}

func (r *RawFileSystem) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	ctx, done := r.requestContext(cancel)
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
	ctx, done := r.requestContext(cancel)
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
	ctx, done := r.requestContext(cancel)
	defer done()
	result, errno := r.adapter.Mknod(ctx, input.NodeId, []byte(name), filesystem.CreateOptions{
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

func (r *RawFileSystem) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	ctx, done := r.requestContext(cancel)
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
	ctx, done := r.requestContext(cancel)
	defer done()
	return fuse.Status(r.adapter.Unlink(ctx, header.NodeId, []byte(name)))
}

func (r *RawFileSystem) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	ctx, done := r.requestContext(cancel)
	defer done()
	return fuse.Status(r.adapter.Rmdir(ctx, header.NodeId, []byte(name)))
}

func (r *RawFileSystem) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) fuse.Status {
	if input.Flags != 0 {
		return fuse.Status(syscall.EOPNOTSUPP)
	}
	ctx, done := r.requestContext(cancel)
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
	ctx, done := r.requestContext(cancel)
	defer done()
	_, errno := r.adapter.GetAttr(ctx, input.NodeId)
	return fuse.Status(errno)
}

func (r *RawFileSystem) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	ctx, done := r.requestContext(cancel)
	defer done()
	for attempt := range createRetryLimit {
		status := r.createOrOpen(ctx, input, []byte(name), out)
		if status == fuse.Status(syscall.EAGAIN) && attempt+1 < createRetryLimit {
			continue
		}
		return status
	}
	return fuse.Status(syscall.ENOENT)
}

func (r *RawFileSystem) createOrOpen(ctx context.Context, input *fuse.CreateIn, name []byte, out *fuse.CreateOut) fuse.Status {
	opts := filesystem.CreateOptions{
		Mode: input.Mode & permissionModeMask,
		UID:  input.Uid,
		GID:  input.Gid,
	}
	result, errno := r.adapter.Create(ctx, input.NodeId, name, opts)
	if errno == syscall.EEXIST && input.Flags&syscall.O_EXCL == 0 {
		// A successful FUSE_CREATE always marks the descriptor newly created.
		// ESTALE makes Linux do_filp_open retry path lookup with LOOKUP_REVAL,
		// preserving normal permission checks, truncation and created status.
		return fuse.Status(syscall.ESTALE)
	}
	if errno != 0 {
		return fuse.ToStatus(errno)
	}
	r.fillEntry(&out.EntryOut, result.Stat)
	out.Fh = result.FH
	return fuse.OK
}

func (r *RawFileSystem) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	ctx, done := r.requestContext(cancel)
	defer done()
	fh, errno := r.adapter.Open(ctx, input.NodeId)
	if errno == 0 {
		out.Fh = fh
	}
	return fuse.Status(errno)
}

func (r *RawFileSystem) Read(cancel <-chan struct{}, input *fuse.ReadIn, _ []byte) (fuse.ReadResult, fuse.Status) {
	ctx, done := r.requestContext(cancel)
	defer done()
	data, errno := r.adapter.Read(ctx, input.NodeId, input.Fh, input.Offset, uint64(input.Size))
	if errno != 0 {
		return nil, fuse.Status(errno)
	}
	return fuse.ReadResultData(data), fuse.OK
}

func (r *RawFileSystem) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	ctx, done := r.requestContext(cancel)
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
	ctx, done := r.requestContext(cancel)
	defer done()
	if errno := r.adapter.Release(ctx, input.NodeId, input.Fh); errno != 0 {
		slog.WarnContext(ctx, "filesystem FUSE release failed", "inode", input.NodeId, "fh", input.Fh, "errno", errno)
	}
}

func (r *RawFileSystem) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	ctx, done := r.requestContext(cancel)
	defer done()
	return fuse.Status(r.adapter.Flush(ctx, input.NodeId, input.Fh))
}

func (r *RawFileSystem) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	ctx, done := r.requestContext(cancel)
	defer done()
	datasync := input.FsyncFlags&1 != 0
	return fuse.Status(r.adapter.Fsync(ctx, input.NodeId, input.Fh, datasync))
}

func (r *RawFileSystem) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	ctx, done := r.requestContext(cancel)
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
	ctx, done := r.requestContext(cancel)
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
	if position.eof {
		return fuse.OK
	}
	result, errno := r.adapter.Readdir(ctx, input.NodeId, position.cookie)
	if errno != 0 {
		return fuse.Status(errno)
	}
	return addDirEntries(handle, input.Offset, position, result, out)
}

func addDirEntries(
	handle *directoryHandle,
	inputOffset uint64,
	position dirPosition,
	result filesystem.ReaddirResult,
	out *fuse.DirEntryList,
) fuse.Status {
	if position.skip > len(result.Entries) {
		return fuse.Status(syscall.EIO)
	}
	offset := inputOffset
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
			handle.positions[offset] = dirPosition{cookie: result.NextCookie, eof: result.NextCookie == ""}
		}
	}
	if len(result.Entries) == 0 && result.NextCookie == "" {
		handle.positions[inputOffset] = dirPosition{eof: true}
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
	ctx, done := r.requestContext(cancel)
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

// requestContext is the context of one FUSE request: it ends when the
// kernel interrupts the request (cancel) or when the server cancels all of
// its requests (cancelInFlight).
func (r *RawFileSystem) requestContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	parent := context.Background()
	if r != nil && r.requests != nil {
		parent = r.requests
	}
	return requestContextWithin(parent, cancel)
}

// cancelInFlight ends the context of every request in flight and of any
// that arrives later. A nil RawFileSystem is a no-op.
func (r *RawFileSystem) cancelInFlight() {
	if r != nil && r.cancelRequests != nil {
		r.cancelRequests()
	}
}

func requestContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	return requestContextWithin(context.Background(), cancel)
}

func requestContextWithin(parent context.Context, cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, done := context.WithCancel(parent)
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
	inner      *fuse.Server
	adapter    *Adapter
	mountPoint string
	// fsType is the mounts-table type of this mount (`fuse.<Name>`, the
	// subtype go-fuse mounts with); the lazy detach refuses to act on
	// anything else at the mount point.
	fsType string
	// fs is the request bridge the kernel mount serves.
	fs *RawFileSystem
	// startCtx is the context MountContext ran under. It bounds only
	// WaitMount: go-fuse's readiness check opens a file on the new mount,
	// the kernel's permission check on the root sends a getattr through to
	// the backend, and that read can block while no leader is available.
	startCtx context.Context
	// released is closed once WaitMount gave up on a cancelled startup and
	// released the mount; Unmount and Serve then return at once.
	released    chan struct{}
	releaseOnce sync.Once
}

// Mount attaches adapter to mountPoint using go-fuse. The returned server is
// ready for Serve; callers should call Unmount during shutdown. It is
// MountContext with a background context; servers that can be told to stop
// during startup should use MountContext.
//
// A dangling FUSE mount left at mountPoint by a server that died without
// unmounting (SIGKILL, a crash) is detached first, because fusermount3
// refuses to mount over a mount point whose transport is not connected. Only
// a mount the mounts table reports with this server's own type
// (`fuse.<Name>`) and whose stat fails with ENOTCONN is touched; a live
// mount or a filesystem of any other type at the path is left alone.
func Mount(mountPoint string, adapter *Adapter, options *fuse.MountOptions) (*Server, error) {
	return MountContext(context.Background(), mountPoint, adapter, options)
}

// MountContext is Mount whose pre-mount work (the stale-mount detach and the
// intent recovery) stops when ctx is cancelled, so a shutdown requested
// during startup does not wait on a hung fusermount helper, an unavailable
// leader, or a long recovery backlog. The returned Server's WaitMount stops
// on ctx too, releasing the mount; otherwise the kernel mount and the
// Server are not bound to ctx, and Unmount ends them.
func MountContext(ctx context.Context, mountPoint string, adapter *Adapter, options *fuse.MountOptions) (*Server, error) {
	if adapter == nil || adapter.core == nil {
		return nil, errors.New("filesystem FUSE adapter is required")
	}
	if strings.TrimSpace(mountPoint) == "" {
		return nil, errors.New("filesystem FUSE mount point is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Wrap(err, "filesystem FUSE mount aborted before start")
	}
	opts := defaultMountOptions(options)
	if _, err := detachStaleMount(ctx, mountPoint, opts.Name, slog.Default()); err != nil {
		return nil, errors.Wrap(err, "detach stale filesystem FUSE mount")
	}
	if recoverer, ok := adapter.core.(intentRecoverer); ok {
		recoveryCtx, cancel := context.WithTimeout(ctx, startupRecoveryTimeout)
		recoveryErr := recoverIntentsBeforeMount(recoveryCtx, recoverer)
		cancel()
		if recoveryErr != nil {
			return nil, errors.Wrap(recoveryErr, "recover filesystem intents before FUSE mount")
		}
	}
	fs := NewRawFileSystem(adapter)
	inner, err := newServerContext(ctx, fs, mountPoint, opts)
	if err != nil {
		return nil, err
	}
	return &Server{
		inner:      inner,
		adapter:    adapter,
		mountPoint: mountPoint,
		fsType:     fuseTypeFor(opts.Name),
		fs:         fs,
		startCtx:   ctx,
		released:   make(chan struct{}),
	}, nil
}

type newServerResult struct {
	server *fuse.Server
	err    error
}

// newServerContext runs fuse.NewServer in its own goroutine so a cancelled
// ctx can unwind startup: go-fuse waits for the fusermount helper with no
// context of its own, and a hung helper would otherwise block the mount, the
// errgroup drain and the WAL sync behind it forever. A mount that completes
// after the caller gave up is unmounted again so no kernel mount leaks.
func newServerContext(ctx context.Context, fs *RawFileSystem, mountPoint string, opts *fuse.MountOptions) (*fuse.Server, error) {
	results := make(chan newServerResult, 1)
	go func() {
		server, err := fuse.NewServer(fs, mountPoint, opts)
		results <- newServerResult{server: server, err: err}
	}()
	select {
	case res := <-results:
		if res.err != nil {
			return nil, errors.Wrap(res.err, "mount filesystem FUSE server")
		}
		return res.server, nil
	case <-ctx.Done():
		go releaseAbandonedMount(results, fs, mountPoint, fuseTypeFor(opts.Name))
		return nil, errors.Wrap(ctx.Err(), "filesystem FUSE mount cancelled while waiting for the mount helper")
	}
}

// abandonedUnmountTimeout and abandonedDetachTimeout split the one window a
// mount whose caller gave up on it is released in. The context-bound plain
// unmount, together with the wait for a cancelled
// readiness check before it, must finish within abandonedUnmountTimeout of
// the start of the release; the lazy detach that may follow must finish
// within abandonedDetachTimeout after that deadline. A release therefore
// never takes longer than their sum, a stop requested during startup
// included. Variables so tests can shorten them.
var (
	abandonedUnmountTimeout = 7 * time.Second
	abandonedDetachTimeout  = 3 * time.Second
)

// releaseAbandonedMount tears down a mount that completed after MountContext
// had already given up on it. It first cancels every request the late mount
// serves, now and later (fs is its request bridge), so a client request that
// reached the backend before the release returns instead of running on
// while the caller's unwinding closes the stores. go-fuse's NewServer
// registers an event loop that only Serve can drain, so Serve must run
// while the context-bound unmount releases the kernel connection.
func releaseAbandonedMount(results <-chan newServerResult, fs *RawFileSystem, mountPoint, fsType string) {
	res := <-results
	if res.server == nil {
		return
	}
	fs.cancelInFlight()
	go res.server.Serve()
	releaseAbandoned(func(ctx context.Context) error { return unmountFUSEMount(ctx, mountPoint, fsType) }, mountPoint, fsType)
}

// releaseAbandoned releases a mount in a release window that starts now
// (see releaseAbandonedBy).
func releaseAbandoned(unmount func(context.Context) error, mountPoint, fsType string) {
	releaseAbandonedBy(time.Now().Add(abandonedUnmountTimeout), unmount, mountPoint, fsType)
}

// releaseAbandonedBy runs unmount until unmountBy. When it fails or does
// not return in time, the mount is detached lazily instead, by
// abandonedDetachTimeout after unmountBy, so no kernel mount is left
// behind; the lazy detach itself refuses a path that no longer holds a
// mount of fsType, so an unmount that did succeed late is not followed by a
// detach of whatever sits underneath. When unmountBy has already passed,
// unmount is not started at all. The callback must reap its context-bound
// helper before returning so nothing can later act on a replacement mount.
func releaseAbandonedBy(unmountBy time.Time, unmount func(context.Context) error, mountPoint, fsType string) {
	ctx, cancel := context.WithDeadline(context.Background(), unmountBy)
	if ctx.Err() != nil {
		slog.Warn("filesystem FUSE unmount of an abandoned mount has no time left, detaching lazily",
			"mount_point", mountPoint,
			"timeout", abandonedUnmountTimeout,
		)
	} else if err := unmount(ctx); err == nil {
		cancel()
		return
	} else {
		slog.Warn("filesystem FUSE unmount of an abandoned mount failed, detaching lazily", "mount_point", mountPoint, "err", err)
	}
	// The context-bound callback reaps its helper before returning.
	cancel()
	ctx, cancel = context.WithDeadline(context.Background(), unmountBy.Add(abandonedDetachTimeout))
	defer cancel()
	if _, err := detachFUSEMount(ctx, mountPoint, fsType); err != nil && !errors.Is(err, errNoFUSEMount) {
		slog.Warn("filesystem FUSE lazy detach of an abandoned mount failed", "mount_point", mountPoint, "err", err)
	}
}

func recoverIntentsBeforeMount(ctx context.Context, recoverer intentRecoverer) error {
	for {
		stats, err := recoverer.RecoverIntents(ctx, startupRecoveryBatch)
		if err != nil {
			return errors.Wrap(err, "drain filesystem recovery batch")
		}
		if stats.MoveJobsResumed == 0 && stats.MoveJobsCleared == 0 && stats.IntentsCleared == 0 {
			return nil
		}
	}
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

// Serve runs the request loop until the kernel mount goes away. After
// WaitMount released the mount of a cancelled startup it returns once that
// release has finished, even when a backend request that ignores its
// context still holds one of go-fuse's loops.
func (s *Server) Serve() {
	if s == nil || s.inner == nil {
		return
	}
	if s.released == nil {
		s.inner.Serve()
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.inner.Serve()
	}()
	select {
	case <-done:
	case <-s.released:
	}
}

// WaitMount waits until the kernel mount answers requests. When the context
// MountContext ran under is cancelled first, it releases the mount (see
// release) and returns the cancellation, so a shutdown requested while the
// readiness check is stuck in a backend request is not held up by it.
// Serve must be running, as for go-fuse's own WaitMount.
func (s *Server) WaitMount() error {
	if s == nil || s.inner == nil {
		return nil
	}
	ready := make(chan error, 1)
	inner := s.inner
	go func() { ready <- inner.WaitMount() }()
	var stop <-chan struct{}
	if s.startCtx != nil {
		stop = s.startCtx.Done()
	}
	select {
	case err := <-ready:
		return errors.WithStack(err)
	case <-stop:
		s.release(ready)
		return errors.Wrap(s.startCtx.Err(), "filesystem FUSE mount cancelled while waiting for it to become ready")
	}
}

// release tears down a mount whose startup was cancelled. It first cancels
// every request in flight: the kernel caller of a request already handed to
// this server waits for the answer uninterruptibly (here go-fuse's readiness
// check in this very process), so the handler must return before anything
// else can. It then waits for the readiness check (ready) and unmounts,
// falling back to the guarded lazy detach, inside one release window (see
// releaseAfterReadiness). The Server is then marked released.
func (s *Server) release(ready <-chan error) {
	s.releaseOnce.Do(func() {
		s.fs.cancelInFlight()
		releaseAfterReadiness(ready, s.UnmountContext, s.mountPoint, s.fsType)
		if s.released != nil {
			close(s.released)
		}
	})
}

// releaseAfterReadiness waits for the cancelled readiness check (ready) and
// then releases the mount with releaseAbandonedBy, all inside one release
// window: the readiness wait and the plain unmount share its unmount
// deadline, so a check that returns late leaves the unmount only what is
// left of it, and the lazy detach runs by its end. A check still running at
// the unmount deadline means a backend request is ignoring its context, and
// the ordinary unmount is skipped for the lazy detach alone.
func releaseAfterReadiness(ready <-chan error, unmount func(context.Context) error, mountPoint, fsType string) {
	unmountBy := time.Now().Add(abandonedUnmountTimeout)
	timer := time.NewTimer(abandonedUnmountTimeout)
	select {
	case <-ready:
	case <-timer.C:
		unmount = func(context.Context) error { return errReadinessStuck }
	}
	timer.Stop()
	releaseAbandonedBy(unmountBy, unmount, mountPoint, fsType)
}

// errReadinessStuck reports that go-fuse's readiness check did not return
// after its backend requests were cancelled, so the plain unmount is skipped.
var errReadinessStuck = errors.New("filesystem FUSE readiness check did not return after cancellation")

// isReleased reports whether WaitMount already released the mount.
func (s *Server) isReleased() bool {
	if s.released == nil {
		return false
	}
	select {
	case <-s.released:
		return true
	default:
		return false
	}
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
	if s.inner == nil || s.isReleased() {
		return nil
	}
	return errors.WithStack(s.inner.Unmount())
}

// UnmountLazy detaches the mount from the namespace even while clients still
// hold files on it open (`fusermount -u -z`, or a lazy unmount(2)): new
// lookups see the plain directory at once, and the kernel releases the FUSE
// connection, ending Serve, once the last open handle is closed. It is the
// fallback for an Unmount that failed with EBUSY or did not return during
// shutdown, so it first stops serving: every request in flight, and every
// one that arrives later, has its context cancelled. A handler blocked in
// the backend (the reason the Unmount hung) therefore returns before the
// stores behind it are closed, and requests through a handle that is still
// open get an error from then on. The detach itself acts only on a mount of
// this server's own type still present at the mount point: when the kernel
// mount is already gone (Unmount removed it and is merely waiting for its
// event loop), or a filesystem of any other type is exposed there, nothing
// is touched and nil is returned. A nil or never-mounted Server is a no-op
// apart from the cancellation.
func (s *Server) UnmountLazy(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.fs.cancelInFlight()
	if s.inner == nil || s.mountPoint == "" {
		return nil
	}
	_, err := detachFUSEMount(ctx, s.mountPoint, s.fsType)
	if errors.Is(err, errNoFUSEMount) {
		slog.InfoContext(ctx, "filesystem FUSE mount is already gone, nothing to detach", "mount_point", s.mountPoint)
		return nil
	}
	return err
}
