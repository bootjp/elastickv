package fuseadapter

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	defaultReaddirLimit             = 128
	defaultHandleKeepaliveInterval  = time.Minute
	defaultHandleKeepaliveTimeout   = 5 * time.Second
	disabledHandleKeepaliveInterval = 0
)

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
	core              Core
	mutations         mutationTracker
	clientID          []byte
	operationSession  filesystem.OperationSessionID
	sessionConflict   bool
	journaledWrites   bool
	journaledMetadata bool
	journaledUnlink   bool
	operationSequence atomic.Uint64
	receiptMu         sync.Mutex
	receipts          map[uint64]map[filesystem.OperationID]struct{}
	readdirLimit      int
	keepaliveInterval time.Duration
	keepaliveTimeout  time.Duration
	keepaliveCancel   context.CancelFunc
	closeOnce         sync.Once
	handlesMu         sync.Mutex
	handles           map[openHandleKey]struct{}
}

type Option func(*Adapter)

type openHandleKey struct {
	inode uint64
	fh    uint64
}

var errnoMappings = []struct {
	err   error
	errno syscall.Errno
}{
	{kv.ErrTransactionOutcomeUnknown, syscall.EIO},
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
	{filesystem.ErrStaleHome, syscall.EAGAIN},
	{filesystem.ErrContentionLimit, syscall.EBUSY},
	{filesystem.ErrOperationQuota, syscall.ENOSPC},
	{store.ErrWriteConflict, syscall.EAGAIN},
}

func WithReaddirLimit(limit int) Option {
	return func(a *Adapter) {
		if limit > 0 {
			a.readdirLimit = limit
		}
	}
}

func WithHandleKeepaliveInterval(interval time.Duration) Option {
	return func(a *Adapter) {
		a.keepaliveInterval = interval
	}
}

func WithHandleKeepaliveTimeout(timeout time.Duration) Option {
	return func(a *Adapter) {
		if timeout > 0 {
			a.keepaliveTimeout = timeout
		}
	}
}

// WithJournaledWrites assigns one durable operation identity to each FUSE
// write request. The caller must register the session before mounting.
func WithJournaledWrites(session filesystem.OperationSessionID) Option {
	return func(a *Adapter) {
		a.setOperationSession(session)
		a.journaledWrites = true
	}
}

// WithJournaledMetadata journals setattr requests that do not change size.
// Size changes need a separate chunk-deletion planner.
func WithJournaledMetadata(session filesystem.OperationSessionID) Option {
	return func(a *Adapter) {
		a.setOperationSession(session)
		a.journaledMetadata = true
	}
}

// WithJournaledUnlink journals file unlink requests under the same session.
func WithJournaledUnlink(session filesystem.OperationSessionID) Option {
	return func(a *Adapter) {
		a.setOperationSession(session)
		a.journaledUnlink = true
	}
}

func (a *Adapter) setOperationSession(session filesystem.OperationSessionID) {
	if a.operationSession != (filesystem.OperationSessionID{}) && a.operationSession != session {
		a.sessionConflict = true
		return
	}
	a.operationSession = session
}

func New(core Core, clientID []byte, opts ...Option) *Adapter {
	a := &Adapter{
		core:              core,
		clientID:          append([]byte(nil), clientID...),
		readdirLimit:      defaultReaddirLimit,
		keepaliveInterval: defaultHandleKeepaliveInterval,
		keepaliveTimeout:  defaultHandleKeepaliveTimeout,
	}
	for _, opt := range opts {
		opt(a)
	}
	if a.keepaliveInterval > disabledHandleKeepaliveInterval {
		ctx, cancel := context.WithCancel(context.Background())
		a.keepaliveCancel = cancel
		go a.keepOpenHandlesAlive(ctx)
	}
	return a
}

func (a *Adapter) Close() {
	a.closeOnce.Do(func() {
		if a.keepaliveCancel != nil {
			a.keepaliveCancel()
		}
	})
}

// Errno maps a backend error to the errno reported to the kernel. Service
// operations absorb definitive OCC conflicts before returning to FUSE; EAGAIN
// remains the legacy fallback for paths not yet integrated with replanning.
func Errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	for _, mapping := range errnoMappings {
		if errors.Is(err, mapping.err) {
			return mapping.errno
		}
	}
	if filesystem.IsWriteConflict(err) {
		return syscall.EAGAIN
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
) (stat filesystem.Stat, errno syscall.Errno) {
	ticket, errno := a.mutations.begin(ctx, inode)
	if errno != 0 {
		return filesystem.Stat{}, errno
	}
	var err error
	defer func() { a.mutations.end(ticket, errors.Is(err, kv.ErrTransactionOutcomeUnknown)) }()
	if a.journaledMetadata && !mask.Size {
		setter, ok := a.core.(interface {
			SetAttrWithOperation(context.Context, filesystem.OperationID, uint64, filesystem.SetAttrMask, filesystem.SetAttr) (filesystem.Stat, error)
		})
		if !ok || a.sessionConflict || a.operationSession.Filesystem == [16]byte{} || a.operationSession.Session == [16]byte{} {
			return filesystem.Stat{}, syscall.EOPNOTSUPP
		}
		id := a.nextOperationID()
		stat, err = setter.SetAttrWithOperation(ctx, id, inode, mask, attrs)
		if err == nil {
			a.recordReceipt(inode, id)
		}
	} else {
		stat, err = a.core.SetAttr(ctx, inode, mask, attrs)
	}
	return stat, Errno(err)
}

func (a *Adapter) nextOperationID() filesystem.OperationID {
	return filesystem.OperationID{Filesystem: a.operationSession.Filesystem, Session: a.operationSession.Session,
		Sequence: a.operationSequence.Add(1)}
}

func (a *Adapter) Open(ctx context.Context, inode uint64) (uint64, syscall.Errno) {
	fh, err := a.core.Open(ctx, inode, a.clientID)
	if err == nil {
		a.trackOpenHandle(inode, fh)
	} else if Errno(err) == syscall.EIO {
		slog.WarnContext(ctx, "filesystem FUSE open failed", "inode", inode, "error", err.Error())
	}
	return fh, Errno(err)
}

func (a *Adapter) Read(ctx context.Context, inode uint64, fh uint64, offset uint64, size uint64) ([]byte, syscall.Errno) {
	if errno := a.refreshOpenHandleLease(ctx, inode, fh); errno != 0 {
		return nil, errno
	}
	data, err := a.core.Read(ctx, inode, fh, offset, size)
	return data, Errno(err)
}

func (a *Adapter) Write(ctx context.Context, inode uint64, fh uint64, offset uint64, data []byte) (n int, errno syscall.Errno) {
	ticket, errno := a.mutations.begin(ctx, inode)
	if errno != 0 {
		return 0, errno
	}
	var writeErr error
	defer func() { a.mutations.end(ticket, errors.Is(writeErr, kv.ErrTransactionOutcomeUnknown)) }()
	if errno := a.refreshOpenHandleLease(ctx, inode, fh); errno != 0 {
		return 0, errno
	}
	if len(data) == 0 {
		return 0, 0
	}
	if a.journaledWrites {
		writer, ok := a.core.(interface {
			WriteWithOperation(context.Context, filesystem.OperationID, uint64, uint64, []byte) (int, error)
		})
		if !ok || a.sessionConflict || a.operationSession.Filesystem == [16]byte{} || a.operationSession.Session == [16]byte{} {
			return 0, syscall.EOPNOTSUPP
		}
		id := a.nextOperationID()
		n, writeErr = writer.WriteWithOperation(ctx, id, inode, offset, data)
		if writeErr == nil {
			a.recordReceipt(inode, id)
		}
	} else {
		n, writeErr = a.core.Write(ctx, inode, fh, offset, data)
	}
	if writeErr != nil && Errno(writeErr) == syscall.EIO {
		slog.WarnContext(ctx, "filesystem FUSE write failed", "inode", inode, "offset", offset, "length", len(data), "error", writeErr.Error())
	}
	return n, Errno(writeErr)
}

func (a *Adapter) Flush(ctx context.Context, inode uint64, fh uint64) syscall.Errno {
	if errno := a.mutations.wait(ctx, inode); errno != 0 {
		return errno
	}
	if errno := a.refreshOpenHandleLease(ctx, inode, fh); errno != 0 {
		return errno
	}
	if _, errno := a.verifyReceipts(ctx, inode); errno != 0 {
		return errno
	}
	return Errno(a.core.Flush(ctx, inode, fh))
}

func (a *Adapter) Fsync(ctx context.Context, inode uint64, fh uint64, datasync bool) syscall.Errno {
	if errno := a.mutations.wait(ctx, inode); errno != 0 {
		return errno
	}
	if errno := a.refreshOpenHandleLease(ctx, inode, fh); errno != 0 {
		return errno
	}
	receipts, errno := a.verifyReceipts(ctx, inode)
	if errno != 0 {
		return errno
	}
	if errno := Errno(a.core.Fsync(ctx, inode, fh, datasync)); errno != 0 {
		return errno
	}
	a.clearReceipts(inode, receipts)
	return 0
}

func (a *Adapter) FsyncDirectory(ctx context.Context, inode uint64) syscall.Errno {
	if errno := a.mutations.wait(ctx, inode); errno != 0 {
		return errno
	}
	receipts, errno := a.verifyReceipts(ctx, inode)
	if errno != 0 {
		return errno
	}
	if errno := Errno(a.core.Fsync(ctx, inode, 0, false)); errno != 0 {
		return errno
	}
	a.clearReceipts(inode, receipts)
	return 0
}

func (a *Adapter) Release(ctx context.Context, inode uint64, fh uint64) syscall.Errno {
	err := a.core.Release(ctx, inode, fh, a.clientID)
	if err != nil && Errno(err) == syscall.EIO {
		slog.WarnContext(ctx, "filesystem FUSE release uncertain", "inode", inode, "fh", fh, "error", err.Error())
	}
	a.untrackOpenHandle(inode, fh)
	return Errno(err)
}

func (a *Adapter) Create(
	ctx context.Context,
	parent uint64,
	name []byte,
	opts filesystem.CreateOptions,
) (result filesystem.CreateResult, errno syscall.Errno) {
	ticket, errno := a.mutations.begin(ctx, parent)
	if errno != 0 {
		return filesystem.CreateResult{}, errno
	}
	var err error
	defer func() { a.mutations.end(ticket, errors.Is(err, kv.ErrTransactionOutcomeUnknown)) }()
	opts.ClientID = append([]byte(nil), a.clientID...)
	result, err = a.core.Create(ctx, parent, name, opts)
	if err == nil {
		a.trackOpenHandle(result.Inode, result.FH)
	}
	return result, Errno(err)
}

func (a *Adapter) Mknod(
	ctx context.Context,
	parent uint64,
	name []byte,
	opts filesystem.CreateOptions,
) (result filesystem.CreateResult, errno syscall.Errno) {
	ticket, errno := a.mutations.begin(ctx, parent)
	if errno != 0 {
		return filesystem.CreateResult{}, errno
	}
	var err error
	defer func() { a.mutations.end(ticket, errors.Is(err, kv.ErrTransactionOutcomeUnknown)) }()
	opts.ClientID = nil
	result, err = a.core.Create(ctx, parent, name, opts)
	return result, Errno(err)
}

func (a *Adapter) Mkdir(
	ctx context.Context,
	parent uint64,
	name []byte,
	opts filesystem.CreateOptions,
) (result filesystem.CreateResult, errno syscall.Errno) {
	ticket, errno := a.mutations.begin(ctx, parent)
	if errno != 0 {
		return filesystem.CreateResult{}, errno
	}
	var err error
	defer func() { a.mutations.end(ticket, errors.Is(err, kv.ErrTransactionOutcomeUnknown)) }()
	result, err = a.core.Mkdir(ctx, parent, name, opts)
	return result, Errno(err)
}

func (a *Adapter) Unlink(ctx context.Context, parent uint64, name []byte) (errno syscall.Errno) {
	ticket, errno := a.mutations.begin(ctx, parent)
	if errno != 0 {
		return errno
	}
	var err error
	defer func() { a.mutations.end(ticket, errors.Is(err, kv.ErrTransactionOutcomeUnknown)) }()
	if a.journaledUnlink {
		unlinker, ok := a.core.(interface {
			UnlinkWithOperation(context.Context, filesystem.OperationID, uint64, []byte) error
		})
		if !ok || a.sessionConflict || a.operationSession.Filesystem == [16]byte{} || a.operationSession.Session == [16]byte{} {
			return syscall.EOPNOTSUPP
		}
		id := a.nextOperationID()
		err = unlinker.UnlinkWithOperation(ctx, id, parent, name)
		if err == nil {
			a.recordReceipt(parent, id)
		}
	} else {
		err = a.core.Unlink(ctx, parent, name)
	}
	return Errno(err)
}

func (a *Adapter) Rmdir(ctx context.Context, parent uint64, name []byte) (errno syscall.Errno) {
	ticket, errno := a.mutations.begin(ctx, parent)
	if errno != 0 {
		return errno
	}
	var err error
	defer func() { a.mutations.end(ticket, errors.Is(err, kv.ErrTransactionOutcomeUnknown)) }()
	err = a.core.Rmdir(ctx, parent, name)
	return Errno(err)
}

func (a *Adapter) Rename(
	ctx context.Context,
	oldParent uint64,
	oldName []byte,
	newParent uint64,
	newName []byte,
) (errno syscall.Errno) {
	ticket, errno := a.mutations.begin(ctx, oldParent, newParent)
	if errno != 0 {
		return errno
	}
	var err error
	defer func() { a.mutations.end(ticket, errors.Is(err, kv.ErrTransactionOutcomeUnknown)) }()
	err = a.core.Rename(ctx, oldParent, oldName, newParent, newName)
	return Errno(err)
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
	err := a.core.RefreshOpenHandleLease(ctx, inode, fh, a.clientID)
	if err != nil && Errno(err) == syscall.EIO {
		slog.WarnContext(ctx, "filesystem FUSE lease refresh failed", "inode", inode, "fh", fh, "error", err.Error())
	}
	return Errno(err)
}

func (a *Adapter) trackOpenHandle(inode uint64, fh uint64) {
	if fh == 0 {
		return
	}
	a.handlesMu.Lock()
	defer a.handlesMu.Unlock()
	if a.handles == nil {
		a.handles = make(map[openHandleKey]struct{})
	}
	a.handles[openHandleKey{inode: inode, fh: fh}] = struct{}{}
}

func (a *Adapter) untrackOpenHandle(inode uint64, fh uint64) {
	if fh == 0 {
		return
	}
	a.handlesMu.Lock()
	defer a.handlesMu.Unlock()
	delete(a.handles, openHandleKey{inode: inode, fh: fh})
}

func (a *Adapter) openHandleSnapshot() []openHandleKey {
	a.handlesMu.Lock()
	defer a.handlesMu.Unlock()
	if len(a.handles) == 0 {
		return nil
	}
	handles := make([]openHandleKey, 0, len(a.handles))
	for handle := range a.handles {
		handles = append(handles, handle)
	}
	return handles
}

func (a *Adapter) keepOpenHandlesAlive(ctx context.Context) {
	ticker := time.NewTicker(a.keepaliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.refreshTrackedOpenHandles(ctx)
		}
	}
}

func (a *Adapter) refreshTrackedOpenHandles(ctx context.Context) {
	for _, handle := range a.openHandleSnapshot() {
		handleCtx, cancel := context.WithTimeout(ctx, a.keepaliveTimeout)
		err := a.core.RefreshOpenHandleLease(handleCtx, handle.inode, handle.fh, a.clientID)
		cancel()
		switch {
		case err == nil:
		case errors.Is(err, filesystem.ErrNotFound):
			a.untrackOpenHandle(handle.inode, handle.fh)
		case ctx.Err() != nil:
			return
		default:
			slog.WarnContext(ctx, "filesystem open handle lease refresh failed",
				"inode", handle.inode,
				"fh", handle.fh,
				"err", err,
			)
		}
	}
}

func (*Adapter) Link(context.Context) syscall.Errno {
	return syscall.EOPNOTSUPP
}

func (*Adapter) Symlink(context.Context) syscall.Errno {
	return syscall.EOPNOTSUPP
}

func (*Adapter) Readlink(context.Context) syscall.Errno {
	return syscall.EOPNOTSUPP
}

func (*Adapter) FileLock(context.Context) syscall.Errno {
	return syscall.EOPNOTSUPP
}
