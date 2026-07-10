package filesystem

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"math"
	"time"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	RootInode uint64 = 1

	bytesPerMiB                     uint64 = 1 << 20
	DefaultChunkSize                       = 4 * bytesPerMiB
	defaultGeneration               uint64 = 1
	initialEpoch                    uint64 = 1
	directoryInitialNlink           uint32 = 2
	fileInitialNlink                uint32 = 1
	rootParentInode                 uint64 = RootInode
	randomRetryLimit                       = 16
	randomUint64Bytes                      = 8
	keyValuePairArity                      = 2
	maxReadSize                            = 64 * bytesPerMiB
	maxScanPageSize                        = 1024
	defaultOpenHandleLeaseTTL              = 5 * time.Minute
	defaultLeaseReaperLimit                = 256
	statFSScanPageSize                     = 512
	chunkDeleteTxnPageSize                 = 512
	chunkDeleteWriteConflictRetries        = 8
	chunkDeleteWriteConflictBackoff        = 10 * time.Millisecond
	openRefScanLimit                       = 2
	openTxnRetryLimit                      = 4
)

var (
	ErrStoreRequired       = errors.New("filesystem: store is required")
	ErrDispatcherRequired  = errors.New("filesystem: dispatcher is required")
	ErrNotFound            = errors.New("filesystem: no such file or directory")
	ErrExists              = errors.New("filesystem: file exists")
	ErrNotDir              = errors.New("filesystem: not a directory")
	ErrIsDir               = errors.New("filesystem: is a directory")
	ErrNotEmpty            = errors.New("filesystem: directory not empty")
	ErrCrossDevice         = errors.New("filesystem: cross-domain rename")
	ErrInvalid             = errors.New("filesystem: invalid argument")
	ErrUnsupported         = errors.New("filesystem: operation not supported")
	ErrInodeCollisionLimit = errors.New("filesystem: inode allocation collision limit reached")
)

type FileType string

const (
	TypeFile      FileType = "file"
	TypeDirectory FileType = "directory"
)

type Dispatcher interface {
	Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error)
}

type linearizableReader interface {
	LinearizableRead(context.Context) (uint64, error)
}

type InodeMeta struct {
	Inode      uint64   `json:"inode"`
	Parent     uint64   `json:"parent"`
	Type       FileType `json:"type"`
	Mode       uint32   `json:"mode"`
	UID        uint32   `json:"uid"`
	GID        uint32   `json:"gid"`
	Size       uint64   `json:"size"`
	AtimeNsec  int64    `json:"atime_nsec"`
	MtimeNsec  int64    `json:"mtime_nsec"`
	CtimeNsec  int64    `json:"ctime_nsec"`
	ChunkSize  uint64   `json:"chunk_size"`
	HomeSlot   uint64   `json:"home_slot"`
	Epoch      uint64   `json:"epoch"`
	Nlink      uint32   `json:"nlink"`
	Generation uint64   `json:"generation"`
	Orphaned   bool     `json:"orphaned,omitempty"`
}

type HomeState string

const (
	HomeStateActive    HomeState = "active"
	HomeStateMigrating HomeState = "migrating"
)

type Home struct {
	HomeSlot uint64    `json:"home_slot"`
	State    HomeState `json:"state"`
	Epoch    uint64    `json:"epoch"`
}

type DirEntry struct {
	Inode uint64   `json:"inode"`
	Type  FileType `json:"type"`
}

type Stat struct {
	Inode      uint64
	Generation uint64
	Type       FileType
	Mode       uint32
	UID        uint32
	GID        uint32
	Size       uint64
	Nlink      uint32
	AtimeNsec  int64
	MtimeNsec  int64
	CtimeNsec  int64
}

type CreateOptions struct {
	Mode     uint32
	UID      uint32
	GID      uint32
	ClientID []byte
}

type CreateResult struct {
	Inode uint64
	FH    uint64
	Stat  Stat
}

type SetAttrMask struct {
	Mode  bool
	UID   bool
	GID   bool
	Size  bool
	Atime bool
	Mtime bool
}

type SetAttr struct {
	Mode      uint32
	UID       uint32
	GID       uint32
	Size      uint64
	AtimeNsec int64
	MtimeNsec int64
}

type Dirent struct {
	Name  []byte
	Inode uint64
	Type  FileType
}

type ReaddirResult struct {
	Entries    []Dirent
	NextCookie string
}

type StatFS struct {
	ChunkSize uint64
	Files     uint64
	FreeFiles uint64
	Capacity  uint64
	Free      uint64
}

type FSUsage struct {
	Files uint64 `json:"files"`
	Bytes uint64 `json:"bytes"`
}

type usageDelta struct {
	filesAdd uint64
	filesSub uint64
	bytesAdd uint64
	bytesSub uint64
}

type chunkDeletePlan struct {
	homeSlot        uint64
	inode           uint64
	start           []byte
	chunkSize       uint64
	protectLiveSize bool
}

type OpenHandleLease struct {
	Inode       uint64 `json:"inode"`
	ClientID    []byte `json:"client_id"`
	FH          uint64 `json:"fh"`
	CreatedNsec int64  `json:"created_nsec"`
	ExpiresNsec int64  `json:"expires_nsec,omitempty"`
}

type LeaseReapStats struct {
	ExpiredRefs        uint64
	OrphanedInodesGCed uint64
}

type Service struct {
	store        store.MVCCStore
	dispatch     Dispatcher
	chunkSize    uint64
	openLeaseTTL time.Duration
	capacity     uint64
	maxFiles     uint64
	now          func() time.Time
	allocID      func() (uint64, error)
	homeSlot     func(uint64) uint64
}

type Option func(*Service)

func WithChunkSize(size uint64) Option {
	return func(s *Service) {
		if size > 0 {
			s.chunkSize = size
		}
	}
}

func WithClock(now func() time.Time) Option {
	return func(s *Service) {
		if now != nil {
			s.now = now
		}
	}
}

func WithIDAllocator(alloc func() (uint64, error)) Option {
	return func(s *Service) {
		if alloc != nil {
			s.allocID = alloc
		}
	}
}

func WithHomeSlotAllocator(homeSlot func(uint64) uint64) Option {
	return func(s *Service) {
		if homeSlot != nil {
			s.homeSlot = homeSlot
		}
	}
}

func WithOpenHandleLeaseTTL(ttl time.Duration) Option {
	return func(s *Service) {
		if ttl >= 0 {
			s.openLeaseTTL = ttl
		}
	}
}

func WithCapacity(capacity uint64) Option {
	return func(s *Service) {
		s.capacity = capacity
	}
}

func WithMaxFiles(maxFiles uint64) Option {
	return func(s *Service) {
		s.maxFiles = maxFiles
	}
}

func NewService(st store.MVCCStore, dispatch Dispatcher, opts ...Option) (*Service, error) {
	if st == nil {
		return nil, ErrStoreRequired
	}
	if dispatch == nil {
		return nil, ErrDispatcherRequired
	}
	s := &Service{
		store:        st,
		dispatch:     dispatch,
		chunkSize:    DefaultChunkSize,
		openLeaseTTL: defaultOpenHandleLeaseTTL,
		now:          time.Now,
		allocID:      randomNonZeroUint64,
		homeSlot: func(inode uint64) uint64 {
			return inode
		},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

func (s *Service) InitializeRoot(ctx context.Context, mode uint32, uid uint32, gid uint32) error {
	ts, err := s.readTS(ctx)
	if err != nil {
		return err
	}
	if _, err := s.inodeAt(ctx, RootInode, ts); err == nil {
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	now := s.now().UnixNano()
	meta := InodeMeta{
		Inode:      RootInode,
		Parent:     rootParentInode,
		Type:       TypeDirectory,
		Mode:       mode,
		UID:        uid,
		GID:        gid,
		AtimeNsec:  now,
		MtimeNsec:  now,
		CtimeNsec:  now,
		ChunkSize:  s.chunkSize,
		HomeSlot:   s.homeSlot(RootInode),
		Epoch:      initialEpoch,
		Nlink:      directoryInitialNlink,
		Generation: defaultGeneration,
	}
	home := Home{HomeSlot: meta.HomeSlot, State: HomeStateActive, Epoch: initialEpoch}
	elems, err := putElems(
		fskeys.InodeKey(RootInode), meta,
		fskeys.HomeKey(RootInode), home,
		fskeys.DirVersionKey(RootInode), unixNsecVersion(now),
		fskeys.UsageKey(), FSUsage{Files: 1},
	)
	if err != nil {
		return err
	}
	_, err = s.dispatch.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems:    elems,
		IsTxn:    true,
		StartTS:  ts,
		ReadKeys: [][]byte{fskeys.InodeKey(RootInode), fskeys.UsageKey()},
	})
	return errors.Wrap(err, "filesystem initialize root dispatch")
}

func (s *Service) Resolve(ctx context.Context, parent uint64, name []byte) (uint64, error) {
	if err := validateName(name); err != nil {
		return 0, err
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return 0, err
	}
	parentMeta, err := s.inodeAt(ctx, parent, ts)
	if err != nil {
		return 0, err
	}
	if parentMeta.Type != TypeDirectory {
		return 0, ErrNotDir
	}
	entry, err := s.dirEntryAt(ctx, parent, name, ts)
	if err != nil {
		return 0, err
	}
	return entry.Inode, nil
}

func (s *Service) GetAttr(ctx context.Context, inode uint64) (Stat, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return Stat{}, err
	}
	meta, err := s.inodeAt(ctx, inode, ts)
	if err != nil {
		return Stat{}, err
	}
	return meta.Stat(), nil
}

//nolint:cyclop // SetAttr mirrors filesystem setattr masks; each branch maps one protocol bit.
func (s *Service) SetAttr(ctx context.Context, inode uint64, mask SetAttrMask, attrs SetAttr) (Stat, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return Stat{}, err
	}
	meta, err := s.inodeAt(ctx, inode, ts)
	if err != nil {
		return Stat{}, err
	}
	elems := make([]*kv.Elem[kv.OP], 0)
	readKeys := [][]byte{fskeys.InodeKey(inode), fskeys.HomeKey(inode)}
	oldSize := meta.Size
	var delta usageDelta
	var chunkDeletes *chunkDeletePlan
	if mask.Size {
		var sizeElems []*kv.Elem[kv.OP]
		var sizeReads [][]byte
		var sizeDelta usageDelta
		ts, meta, sizeElems, sizeReads, sizeDelta, chunkDeletes, oldSize, err = s.setAttrSizeTxnParts(ctx, inode, attrs.Size, ts, meta)
		if err != nil {
			return Stat{}, err
		}
		elems = append(elems, sizeElems...)
		readKeys = append(readKeys, sizeReads...)
		delta = delta.merge(sizeDelta)
	}
	now := s.now().UnixNano()
	if mask.Mode {
		meta.Mode = attrs.Mode
	}
	if mask.UID {
		meta.UID = attrs.UID
	}
	if mask.GID {
		meta.GID = attrs.GID
	}
	if mask.Atime {
		meta.AtimeNsec = attrs.AtimeNsec
	}
	if mask.Mtime {
		meta.MtimeNsec = attrs.MtimeNsec
	} else if mask.Size && attrs.Size != oldSize {
		meta.MtimeNsec = now
	}
	meta.CtimeNsec = now
	elem, err := putElem(fskeys.InodeKey(inode), meta)
	if err != nil {
		return Stat{}, err
	}
	elems = append(elems, elem)
	elems, readKeys, err = s.appendUsageUpdate(ctx, ts, elems, readKeys, delta)
	if err != nil {
		return Stat{}, err
	}
	if err := s.dispatchTxn(ctx, ts, elems, readKeys); err != nil {
		return Stat{}, err
	}
	if err := s.deleteChunkPages(ctx, chunkDeletes); err != nil {
		return Stat{}, err
	}
	return meta.Stat(), nil
}

func (s *Service) setAttrSizeTxnParts(
	ctx context.Context,
	inode uint64,
	size uint64,
	ts uint64,
	meta *InodeMeta,
) (uint64, *InodeMeta, []*kv.Elem[kv.OP], [][]byte, usageDelta, *chunkDeletePlan, uint64, error) {
	if meta.Type == TypeDirectory {
		return 0, nil, nil, nil, usageDelta{}, nil, 0, ErrIsDir
	}
	oldSize := meta.Size
	var err error
	var refreshed bool
	ts, meta, refreshed, err = s.cleanupNonShrinkingSizeChange(ctx, inode, size, meta, ts)
	if err != nil {
		return 0, nil, nil, nil, usageDelta{}, nil, 0, err
	}
	if refreshed {
		oldSize = meta.Size
	}
	chunkSize := meta.effectiveChunkSize(s.chunkSize)
	var elems []*kv.Elem[kv.OP]
	var readKeys [][]byte
	var delta usageDelta
	var chunkDeletes *chunkDeletePlan
	if size < meta.Size {
		elems, readKeys, delta, chunkDeletes, err = s.truncateChunkElems(ctx, meta, ts, size, chunkSize)
		if err != nil {
			return 0, nil, nil, nil, usageDelta{}, nil, 0, err
		}
	}
	meta.Size = size
	return ts, meta, elems, readKeys, delta, chunkDeletes, oldSize, nil
}

func (s *Service) Create(ctx context.Context, parent uint64, name []byte, opts CreateOptions) (CreateResult, error) {
	return s.createNode(ctx, parent, name, TypeFile, opts)
}

func (s *Service) Mkdir(ctx context.Context, parent uint64, name []byte, opts CreateOptions) (CreateResult, error) {
	return s.createNode(ctx, parent, name, TypeDirectory, opts)
}

func (s *Service) Open(ctx context.Context, inode uint64, clientID []byte) (uint64, error) {
	fh, err := s.allocID()
	if err != nil {
		return 0, err
	}
	var lastErr error
	for range openTxnRetryLimit {
		if err := s.openWithHandle(ctx, inode, clientID, fh); err != nil {
			if errors.Is(err, store.ErrWriteConflict) {
				lastErr = err
				continue
			}
			return 0, err
		}
		return fh, nil
	}
	return 0, lastErr
}

func (s *Service) openWithHandle(ctx context.Context, inode uint64, clientID []byte, fh uint64) error {
	ts, err := s.readTS(ctx)
	if err != nil {
		return err
	}
	if _, err := s.inodeAt(ctx, inode, ts); err != nil {
		return err
	}
	refElem, err := s.refPutElem(inode, clientID, fh)
	if err != nil {
		return err
	}
	fenceElem, err := s.refFenceElem(inode)
	if err != nil {
		return err
	}
	return s.dispatchTxn(ctx, ts, []*kv.Elem[kv.OP]{refElem, fenceElem}, [][]byte{
		fskeys.InodeKey(inode),
		fskeys.RefFenceKey(inode),
	})
}

//nolint:cyclop // Read handles EOF, sparse chunks, and chunk boundary copying in one linear path.
func (s *Service) Read(ctx context.Context, inode uint64, _ uint64, offset uint64, size uint64) ([]byte, error) {
	if size > maxReadSize {
		return nil, ErrInvalid
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return nil, err
	}
	meta, err := s.inodeAt(ctx, inode, ts)
	if err != nil {
		return nil, err
	}
	if meta.Type == TypeDirectory {
		return nil, ErrIsDir
	}
	if offset >= meta.Size || size == 0 {
		return []byte{}, nil
	}
	end, err := boundedEnd(offset, size, meta.Size)
	if err != nil {
		return nil, err
	}
	outLen, err := checkedInt(end - offset)
	if err != nil {
		return nil, err
	}
	out := make([]byte, outLen)
	if len(out) == 0 {
		return out, nil
	}
	chunkSize := meta.effectiveChunkSize(s.chunkSize)
	home := meta.HomeSlot
	first := offset / chunkSize
	last := (end - 1) / chunkSize
	for idx := first; idx <= last; idx++ {
		chunkStart := idx * chunkSize
		readStart := max(offset, chunkStart)
		readEnd := min(end, chunkStart+chunkSize)
		raw, getErr := s.store.GetAt(ctx, fskeys.ChunkKey(home, inode, idx), ts)
		if getErr != nil {
			if errors.Is(getErr, store.ErrKeyNotFound) {
				continue
			}
			return nil, errors.Wrap(getErr, "filesystem read chunk")
		}
		chunkOff, err := checkedInt(readStart - chunkStart)
		if err != nil {
			return nil, err
		}
		outOff, err := checkedInt(readStart - offset)
		if err != nil {
			return nil, err
		}
		n, err := checkedInt(readEnd - readStart)
		if err != nil {
			return nil, err
		}
		if chunkOff >= len(raw) {
			continue
		}
		copy(out[outOff:outOff+n], raw[chunkOff:min(len(raw), chunkOff+n)])
	}
	return out, nil
}

//nolint:cyclop // Write performs chunk read-modify-write and metadata update atomically.
func (s *Service) Write(ctx context.Context, inode uint64, _ uint64, offset uint64, data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	writeLen := uint64(len(data))
	end, overflow := addUint64(offset, writeLen)
	if overflow {
		return 0, ErrInvalid
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return 0, err
	}
	meta, err := s.inodeAt(ctx, inode, ts)
	if err != nil {
		return 0, err
	}
	if meta.Type == TypeDirectory {
		return 0, ErrIsDir
	}
	if end > meta.Size {
		ts, meta, _, err = s.cleanupNonShrinkingSizeChange(ctx, inode, end, meta, ts)
		if err != nil {
			return 0, err
		}
	}
	chunkSize := meta.effectiveChunkSize(s.chunkSize)
	first := offset / chunkSize
	last := (end - 1) / chunkSize
	elemCap, err := checkedInt(last - first + uint64(directoryInitialNlink))
	if err != nil {
		return 0, err
	}
	chunkLen, err := checkedInt(chunkSize)
	if err != nil {
		return 0, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, elemCap)
	readKeys := [][]byte{fskeys.InodeKey(inode), fskeys.HomeKey(inode)}
	var delta usageDelta
	for idx := first; idx <= last; idx++ {
		chunkKey := fskeys.ChunkKey(meta.HomeSlot, inode, idx)
		chunkStart := idx * chunkSize
		writeStart := max(offset, chunkStart)
		writeEnd := min(end, chunkStart+chunkSize)
		srcStart, err := checkedInt(writeStart - offset)
		if err != nil {
			return 0, err
		}
		srcEnd, err := checkedInt(writeEnd - offset)
		if err != nil {
			return 0, err
		}
		fullChunkWrite := writeStart == chunkStart && writeEnd == chunkStart+chunkSize
		oldLen, value, readChunk, err := s.writeChunkValue(
			ctx, ts, chunkKey, chunkLen, fullChunkWrite, data, srcStart, srcEnd, writeStart, chunkStart,
		)
		if err != nil {
			return 0, err
		}
		if readChunk {
			readKeys = append(readKeys, chunkKey)
		}
		delta = delta.merge(byteLenDelta(oldLen, uint64(len(value))))
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: chunkKey, Value: value})
	}
	now := s.now().UnixNano()
	if end > meta.Size {
		meta.Size = end
	}
	meta.MtimeNsec = now
	meta.CtimeNsec = now
	elem, err := putElem(fskeys.InodeKey(inode), meta)
	if err != nil {
		return 0, err
	}
	elems = append(elems, elem)
	elems, readKeys, err = s.appendUsageUpdate(ctx, ts, elems, readKeys, delta)
	if err != nil {
		return 0, err
	}
	if err := s.dispatchTxn(ctx, ts, elems, readKeys); err != nil {
		return 0, err
	}
	return len(data), nil
}

func (s *Service) writeChunkValue(
	ctx context.Context,
	ts uint64,
	chunkKey []byte,
	chunkLen int,
	fullChunkWrite bool,
	data []byte,
	srcStart int,
	srcEnd int,
	writeStart uint64,
	chunkStart uint64,
) (uint64, []byte, bool, error) {
	raw, getErr := s.store.GetAt(ctx, chunkKey, ts)
	if getErr != nil && !errors.Is(getErr, store.ErrKeyNotFound) {
		return 0, nil, false, errors.Wrap(getErr, "filesystem read chunk for write")
	}
	oldLen := uint64(len(raw))
	if fullChunkWrite {
		return oldLen, trimChunk(data[srcStart:srcEnd]), false, nil
	}
	chunk := make([]byte, chunkLen)
	copy(chunk, raw)
	dstStart, err := checkedInt(writeStart - chunkStart)
	if err != nil {
		return 0, nil, false, err
	}
	copy(chunk[dstStart:dstStart+(srcEnd-srcStart)], data[srcStart:srcEnd])
	return oldLen, trimChunk(chunk), true, nil
}

func (s *Service) Truncate(ctx context.Context, inode uint64, size uint64) error {
	ts, err := s.readTS(ctx)
	if err != nil {
		return err
	}
	meta, err := s.inodeAt(ctx, inode, ts)
	if err != nil {
		return err
	}
	if meta.Type == TypeDirectory {
		return ErrIsDir
	}
	ts, meta, _, err = s.cleanupNonShrinkingSizeChange(ctx, inode, size, meta, ts)
	if err != nil {
		return err
	}
	chunkSize := meta.effectiveChunkSize(s.chunkSize)
	elems := make([]*kv.Elem[kv.OP], 0)
	readKeys := [][]byte{fskeys.InodeKey(inode), fskeys.HomeKey(inode)}
	var delta usageDelta
	var chunkDeletes *chunkDeletePlan
	if size < meta.Size {
		truncateElems, truncateReads, truncateDelta, plan, err := s.truncateChunkElems(ctx, meta, ts, size, chunkSize)
		if err != nil {
			return err
		}
		elems = append(elems, truncateElems...)
		readKeys = append(readKeys, truncateReads...)
		delta = delta.merge(truncateDelta)
		chunkDeletes = plan
	}
	now := s.now().UnixNano()
	meta.Size = size
	meta.MtimeNsec = now
	meta.CtimeNsec = now
	elem, err := putElem(fskeys.InodeKey(inode), meta)
	if err != nil {
		return err
	}
	elems = append(elems, elem)
	elems, readKeys, err = s.appendUsageUpdate(ctx, ts, elems, readKeys, delta)
	if err != nil {
		return err
	}
	if err := s.dispatchTxn(ctx, ts, elems, readKeys); err != nil {
		return err
	}
	return s.deleteChunkPages(ctx, chunkDeletes)
}

func (s *Service) Unlink(ctx context.Context, parent uint64, name []byte) error {
	return s.unlink(ctx, parent, name, false)
}

func (s *Service) Rmdir(ctx context.Context, parent uint64, name []byte) error {
	return s.unlink(ctx, parent, name, true)
}

//nolint:cyclop // Rename maps filesystem replace/error semantics into one atomic directory update.
func (s *Service) Rename(ctx context.Context, oldParent uint64, oldName []byte, newParent uint64, newName []byte) error {
	if oldParent != newParent {
		return ErrCrossDevice
	}
	if err := validateName(oldName); err != nil {
		return err
	}
	if err := validateName(newName); err != nil {
		return err
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return err
	}
	parentMeta, err := s.inodeAt(ctx, oldParent, ts)
	if err != nil {
		return err
	}
	if parentMeta.Type != TypeDirectory {
		return ErrNotDir
	}
	oldEntry, err := s.dirEntryAt(ctx, oldParent, oldName, ts)
	if err != nil {
		return err
	}
	if bytes.Equal(oldName, newName) {
		return nil
	}
	replacedMeta, err := s.renameReplacedFileMeta(ctx, oldParent, newName, oldEntry, ts)
	if err != nil {
		return err
	}
	now := s.now().UnixNano()
	parentMeta.MtimeNsec = now
	parentMeta.CtimeNsec = now
	newEntry := DirEntry{Inode: oldEntry.Inode, Type: oldEntry.Type}
	putNew, err := putElem(fskeys.DirEntryKey(oldParent, newName), newEntry)
	if err != nil {
		return err
	}
	putVersion, err := putElem(fskeys.DirVersionKey(oldParent), unixNsecVersion(now))
	if err != nil {
		return err
	}
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: fskeys.DirEntryKey(oldParent, oldName)},
		putNew,
		putVersion,
	}
	readKeys := [][]byte{
		fskeys.InodeKey(oldParent),
		fskeys.DirEntryKey(oldParent, oldName),
		fskeys.DirEntryKey(oldParent, newName),
	}
	var delta usageDelta
	var chunkDeletes *chunkDeletePlan
	replacedElems, replacedReads, replacedDelta, plan, err := s.renameTargetTxnParts(ctx, ts, parentMeta, replacedMeta, now)
	if err != nil {
		return err
	}
	elems = append(elems, replacedElems...)
	readKeys = append(readKeys, replacedReads...)
	delta = delta.merge(replacedDelta)
	chunkDeletes = plan
	putParent, err := putElem(fskeys.InodeKey(oldParent), parentMeta)
	if err != nil {
		return err
	}
	elems = append(elems, putParent)
	elems, readKeys, err = s.appendUsageUpdate(ctx, ts, elems, readKeys, delta)
	if err != nil {
		return err
	}
	if err := s.dispatchTxn(ctx, ts, elems, readKeys); err != nil {
		return err
	}
	return s.deleteChunkPages(ctx, chunkDeletes)
}

func (s *Service) renameTargetTxnParts(
	ctx context.Context,
	ts uint64,
	parentMeta *InodeMeta,
	replacedMeta *InodeMeta,
	now int64,
) ([]*kv.Elem[kv.OP], [][]byte, usageDelta, *chunkDeletePlan, error) {
	if replacedMeta == nil {
		return nil, nil, usageDelta{}, nil, nil
	}
	if replacedMeta.Type == TypeDirectory {
		elems, reads, delta, err := s.rmdirTargetTxnParts(ctx, ts, parentMeta, replacedMeta)
		return elems, reads, delta, nil, err
	}
	return s.unlinkFileTxnParts(ctx, ts, replacedMeta, now)
}

func (s *Service) rmdirTargetTxnParts(
	ctx context.Context,
	ts uint64,
	parentMeta *InodeMeta,
	meta *InodeMeta,
) ([]*kv.Elem[kv.OP], [][]byte, usageDelta, error) {
	if err := s.ensureDirectoryEmpty(ctx, meta.Inode, ts); err != nil {
		return nil, nil, usageDelta{}, err
	}
	if parentMeta.Nlink > fileInitialNlink {
		parentMeta.Nlink--
	}
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: fskeys.InodeKey(meta.Inode)},
		{Op: kv.Del, Key: fskeys.HomeKey(meta.Inode)},
		{Op: kv.Del, Key: fskeys.DirVersionKey(meta.Inode)},
	}
	readKeys := [][]byte{
		fskeys.HomeKey(meta.Inode),
		fskeys.DirVersionKey(meta.Inode),
	}
	return elems, readKeys, usageDelta{filesSub: 1}, nil
}

func (s *Service) renameReplacedFileMeta(
	ctx context.Context,
	parent uint64,
	name []byte,
	source DirEntry,
	ts uint64,
) (*InodeMeta, error) {
	replacedEntry, err := s.dirEntryAt(ctx, parent, name, ts)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if err := validateRenameReplacement(source.Type, replacedEntry.Type); err != nil {
		return nil, err
	}
	if replacedEntry.Inode == source.Inode {
		return nil, nil
	}
	meta, err := s.inodeAt(ctx, replacedEntry.Inode, ts)
	if err != nil {
		return nil, err
	}
	if err := validateRenameReplacement(source.Type, meta.Type); err != nil {
		return nil, err
	}
	return meta, nil
}

func validateRenameReplacement(sourceType, targetType FileType) error {
	if sourceType == TypeDirectory {
		if targetType != TypeDirectory {
			return ErrNotDir
		}
		return nil
	}
	if targetType == TypeDirectory {
		return ErrIsDir
	}
	return nil
}

//nolint:cyclop // Readdir has explicit dot-entry and paginated-cookie state transitions.
func (s *Service) Readdir(ctx context.Context, inode uint64, cookie string, limit int) (ReaddirResult, error) {
	if limit <= 0 {
		return ReaddirResult{}, nil
	}
	if limit > maxScanPageSize {
		limit = maxScanPageSize
	}
	state, err := s.readdirState(ctx, cookie)
	if err != nil {
		return ReaddirResult{}, err
	}
	meta, err := s.inodeAt(ctx, inode, state.ReadTS)
	if err != nil {
		return ReaddirResult{}, err
	}
	if meta.Type != TypeDirectory {
		return ReaddirResult{}, ErrNotDir
	}
	result := ReaddirResult{Entries: make([]Dirent, 0, limit)}
	for state.Dot < directoryInitialNlink && len(result.Entries) < limit {
		if state.Dot == 0 {
			result.Entries = append(result.Entries, Dirent{Name: []byte("."), Inode: inode, Type: TypeDirectory})
		} else {
			result.Entries = append(result.Entries, Dirent{Name: []byte(".."), Inode: meta.Parent, Type: TypeDirectory})
		}
		state.Dot++
	}
	if len(result.Entries) < limit {
		if err := s.appendDirectoryEntries(ctx, inode, &state, limit-len(result.Entries), &result); err != nil {
			return ReaddirResult{}, err
		}
	}
	if len(result.Entries) == limit {
		next, err := encodeReaddirCookie(state)
		if err != nil {
			return ReaddirResult{}, err
		}
		result.NextCookie = next
	}
	return result, nil
}

func (s *Service) Flush(context.Context, uint64, uint64) error {
	return nil
}

func (s *Service) Fsync(ctx context.Context, inode uint64, _ uint64, _ bool) error {
	_, err := s.GetAttr(ctx, inode)
	return err
}

func (s *Service) Release(ctx context.Context, inode uint64, fh uint64, clientID []byte) error {
	ts, err := s.readTS(ctx)
	if err != nil {
		return err
	}
	refKey := fskeys.RefKey(inode, clientID, fh)
	elems, readKeys, delta, chunkDeletes, _, err := s.releaseRefTxnParts(ctx, ts, inode, refKey)
	if err != nil {
		return err
	}
	elems, readKeys, err = s.appendUsageUpdate(ctx, ts, elems, readKeys, delta)
	if err != nil {
		return err
	}
	if err := s.dispatchTxn(ctx, ts, elems, readKeys); err != nil {
		return err
	}
	return s.deleteChunkPages(ctx, chunkDeletes)
}

func (s *Service) releaseRefTxnParts(
	ctx context.Context,
	ts uint64,
	inode uint64,
	refKey []byte,
) ([]*kv.Elem[kv.OP], [][]byte, usageDelta, *chunkDeletePlan, bool, error) {
	elems := []*kv.Elem[kv.OP]{{Op: kv.Del, Key: refKey}}
	readKeys := [][]byte{refKey, fskeys.InodeKey(inode)}
	meta, err := s.inodeAt(ctx, inode, ts)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return elems, readKeys, usageDelta{}, nil, false, nil
		}
		return nil, nil, usageDelta{}, nil, false, err
	}
	hasOther, refReads, err := s.hasOtherOpenRefs(ctx, inode, ts, refKey)
	if err != nil {
		return nil, nil, usageDelta{}, nil, false, err
	}
	readKeys = append(readKeys, refReads...)
	if !meta.Orphaned || meta.Nlink != 0 || hasOther {
		return elems, readKeys, usageDelta{}, nil, false, nil
	}
	gcElems, gcReads, delta, plan, err := s.gcFileElems(ctx, meta, ts)
	if err != nil {
		return nil, nil, usageDelta{}, nil, false, err
	}
	elems = append(elems, gcElems...)
	readKeys = append(readKeys, gcReads...)
	return elems, readKeys, delta, plan, true, nil
}

func (s *Service) StatFS(ctx context.Context, _ uint64) (StatFS, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return StatFS{}, err
	}
	usage, err := s.usageAt(ctx, ts)
	if err != nil {
		return StatFS{}, err
	}
	return StatFS{
		ChunkSize: s.chunkSize,
		Files:     usage.Files,
		FreeFiles: freeAfterUsed(s.maxFiles, usage.Files, math.MaxUint64),
		Capacity:  s.capacity,
		Free:      freeAfterUsed(s.capacity, usage.Bytes, 0),
	}, nil
}

type statFSUsage struct {
	files uint64
	bytes uint64
}

func (s *Service) statFSUsage(ctx context.Context, ts uint64) (statFSUsage, error) {
	files, err := s.countVisiblePrefix(ctx, fskeys.InodeAllPrefix(), ts)
	if err != nil {
		return statFSUsage{}, errors.Wrap(err, "filesystem count inodes")
	}
	bytes, err := s.sumVisibleValueBytes(ctx, fskeys.ChunkAllPrefix(), ts)
	if err != nil {
		return statFSUsage{}, errors.Wrap(err, "filesystem sum chunk bytes")
	}
	return statFSUsage{files: files, bytes: bytes}, nil
}

func (s *Service) usageAt(ctx context.Context, ts uint64) (FSUsage, error) {
	raw, err := s.store.GetAt(ctx, fskeys.UsageKey(), ts)
	if err == nil {
		return decodeJSON[FSUsage](raw)
	}
	if !errors.Is(err, store.ErrKeyNotFound) {
		return FSUsage{}, errors.Wrap(err, "filesystem read usage")
	}
	usage, err := s.statFSUsage(ctx, ts)
	if err != nil {
		return FSUsage{}, err
	}
	return FSUsage{Files: usage.files, Bytes: usage.bytes}, nil
}

func (s *Service) appendUsageUpdate(
	ctx context.Context,
	ts uint64,
	elems []*kv.Elem[kv.OP],
	readKeys [][]byte,
	delta usageDelta,
) ([]*kv.Elem[kv.OP], [][]byte, error) {
	if delta.empty() {
		return elems, readKeys, nil
	}
	usage, err := s.usageAt(ctx, ts)
	if err != nil {
		return nil, nil, err
	}
	if err := usage.apply(delta); err != nil {
		return nil, nil, err
	}
	elem, err := putElem(fskeys.UsageKey(), usage)
	if err != nil {
		return nil, nil, err
	}
	return append(elems, elem), append(readKeys, fskeys.UsageKey()), nil
}

func (s *Service) dispatchUsageTxnAndDeleteChunks(
	ctx context.Context,
	ts uint64,
	elems []*kv.Elem[kv.OP],
	readKeys [][]byte,
	delta usageDelta,
	chunkDeletes *chunkDeletePlan,
) error {
	elems, readKeys, err := s.appendUsageUpdate(ctx, ts, elems, readKeys, delta)
	if err != nil {
		return err
	}
	if err := s.dispatchTxn(ctx, ts, elems, readKeys); err != nil {
		return err
	}
	return s.deleteChunkPages(ctx, chunkDeletes)
}

func (d usageDelta) empty() bool {
	return d.filesAdd == 0 && d.filesSub == 0 && d.bytesAdd == 0 && d.bytesSub == 0
}

func (d usageDelta) merge(other usageDelta) usageDelta {
	d.filesAdd += other.filesAdd
	d.filesSub += other.filesSub
	d.bytesAdd += other.bytesAdd
	d.bytesSub += other.bytesSub
	return d
}

func (u *FSUsage) apply(delta usageDelta) error {
	var overflow bool
	if u.Files < delta.filesSub || u.Bytes < delta.bytesSub {
		return ErrInvalid
	}
	u.Files -= delta.filesSub
	u.Bytes -= delta.bytesSub
	u.Files, overflow = addUint64(u.Files, delta.filesAdd)
	if overflow {
		return ErrInvalid
	}
	u.Bytes, overflow = addUint64(u.Bytes, delta.bytesAdd)
	if overflow {
		return ErrInvalid
	}
	return nil
}

func byteLenDelta(oldLen, newLen uint64) usageDelta {
	if newLen >= oldLen {
		return usageDelta{bytesAdd: newLen - oldLen}
	}
	return usageDelta{bytesSub: oldLen - newLen}
}

func (s *Service) countVisiblePrefix(ctx context.Context, prefix []byte, ts uint64) (uint64, error) {
	var count uint64
	err := s.scanVisiblePrefix(ctx, prefix, ts, func(*store.KVPair) error {
		count++
		return nil
	})
	return count, err
}

func (s *Service) sumVisibleValueBytes(ctx context.Context, prefix []byte, ts uint64) (uint64, error) {
	var total uint64
	err := s.scanVisiblePrefix(ctx, prefix, ts, func(pair *store.KVPair) error {
		next, overflow := addUint64(total, uint64(len(pair.Value)))
		if overflow {
			return ErrInvalid
		}
		total = next
		return nil
	})
	return total, err
}

func (s *Service) scanVisiblePrefix(
	ctx context.Context,
	prefix []byte,
	ts uint64,
	visit func(*store.KVPair) error,
) error {
	start := append([]byte(nil), prefix...)
	end := prefixEnd(prefix)
	for {
		page, err := s.store.ScanAt(ctx, start, end, statFSScanPageSize, ts)
		if err != nil {
			return errors.Wrap(err, "filesystem scan prefix")
		}
		if len(page) == 0 {
			return nil
		}
		for _, pair := range page {
			if err := visit(pair); err != nil {
				return err
			}
		}
		if len(page) < statFSScanPageSize {
			return nil
		}
		start = scanCursorAfter(page[len(page)-1].Key)
	}
}

func scanCursorAfter(key []byte) []byte {
	out := append([]byte(nil), key...)
	return append(out, 0)
}

func freeAfterUsed(total uint64, used uint64, unknown uint64) uint64 {
	if total == 0 {
		return unknown
	}
	if used >= total {
		return 0
	}
	return total - used
}

func (s *Service) RefreshOpenHandleLease(ctx context.Context, inode uint64, fh uint64, clientID []byte) error {
	ts, err := s.readTS(ctx)
	if err != nil {
		return err
	}
	if _, err := s.inodeAt(ctx, inode, ts); err != nil {
		return err
	}
	refKey := fskeys.RefKey(inode, clientID, fh)
	if _, err := s.store.GetAt(ctx, refKey, ts); err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return ErrNotFound
		}
		return errors.Wrap(err, "filesystem read open handle lease")
	}
	elem, err := s.refPutElem(inode, clientID, fh)
	if err != nil {
		return err
	}
	return s.dispatchTxn(ctx, ts, []*kv.Elem[kv.OP]{elem}, [][]byte{fskeys.InodeKey(inode), refKey})
}

func (s *Service) ReapExpiredOpenHandleLeases(ctx context.Context, limit int) (LeaseReapStats, error) {
	if limit <= 0 {
		limit = defaultLeaseReaperLimit
	}
	pageLimit := min(limit, maxScanPageSize)
	readTS, err := s.readTS(ctx)
	if err != nil {
		return LeaseReapStats{}, err
	}
	var stats LeaseReapStats
	reapedCount := 0
	nowNsec := s.now().UnixNano()
	err = s.scanOpenHandleLeasePages(ctx, readTS, pageLimit, func(pair *store.KVPair) (bool, error) {
		reaped, gcd, err := s.reapExpiredOpenHandleLease(ctx, pair, nowNsec)
		if err != nil {
			return false, err
		}
		if reaped {
			reapedCount++
			stats.ExpiredRefs++
		}
		if gcd {
			stats.OrphanedInodesGCed++
		}
		return reapedCount >= limit, nil
	})
	return stats, err
}

func (s *Service) scanOpenHandleLeasePages(
	ctx context.Context,
	readTS uint64,
	pageLimit int,
	visit func(*store.KVPair) (bool, error),
) error {
	prefix := fskeys.RefAllPrefix()
	start := append([]byte(nil), prefix...)
	end := prefixEnd(prefix)
	for {
		pairs, err := s.store.ScanAt(ctx, start, end, pageLimit, readTS)
		if err != nil {
			return errors.Wrap(err, "filesystem scan open handle leases")
		}
		if len(pairs) == 0 {
			return nil
		}
		for _, pair := range pairs {
			stop, err := visit(pair)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
		if len(pairs) < pageLimit {
			return nil
		}
		start = scanCursorAfter(pairs[len(pairs)-1].Key)
	}
}

func (s *Service) reapExpiredOpenHandleLease(
	ctx context.Context,
	pair *store.KVPair,
	nowNsec int64,
) (bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return false, false, errors.WithStack(err)
	}
	lease, err := decodeJSON[OpenHandleLease](pair.Value)
	if err != nil {
		return false, false, err
	}
	if !lease.expired(nowNsec) {
		return false, false, nil
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return false, false, err
	}
	current, ok, err := s.openHandleLeaseAt(ctx, pair.Key, ts)
	if err != nil {
		return false, false, err
	}
	if !ok || !current.expired(nowNsec) {
		return false, false, nil
	}
	elems, readKeys, delta, chunkDeletes, gcd, err := s.releaseRefTxnParts(ctx, ts, current.Inode, pair.Key)
	if err != nil {
		return false, false, err
	}
	if err := s.dispatchUsageTxnAndDeleteChunks(ctx, ts, elems, readKeys, delta, chunkDeletes); err != nil {
		return false, false, err
	}
	return true, gcd, nil
}

func (l OpenHandleLease) expired(nowNsec int64) bool {
	return l.ExpiresNsec != 0 && l.ExpiresNsec <= nowNsec
}

func (m InodeMeta) Stat() Stat {
	return Stat{
		Inode:      m.Inode,
		Generation: m.Generation,
		Type:       m.Type,
		Mode:       m.Mode,
		UID:        m.UID,
		GID:        m.GID,
		Size:       m.Size,
		Nlink:      m.Nlink,
		AtimeNsec:  m.AtimeNsec,
		MtimeNsec:  m.MtimeNsec,
		CtimeNsec:  m.CtimeNsec,
	}
}

//nolint:cyclop // createNode handles common file/dir create metadata and optional open semantics.
func (s *Service) createNode(ctx context.Context, parent uint64, name []byte, typ FileType, opts CreateOptions) (CreateResult, error) {
	if err := validateName(name); err != nil {
		return CreateResult{}, err
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return CreateResult{}, err
	}
	parentMeta, err := s.inodeAt(ctx, parent, ts)
	if err != nil {
		return CreateResult{}, err
	}
	if parentMeta.Type != TypeDirectory {
		return CreateResult{}, ErrNotDir
	}
	if _, err := s.dirEntryAt(ctx, parent, name, ts); err == nil {
		return CreateResult{}, ErrExists
	} else if !errors.Is(err, ErrNotFound) {
		return CreateResult{}, err
	}
	inode, err := s.allocateInode(ctx, ts)
	if err != nil {
		return CreateResult{}, err
	}
	now := s.now().UnixNano()
	homeSlot := s.homeSlot(inode)
	nlink := fileInitialNlink
	if typ == TypeDirectory {
		nlink = directoryInitialNlink
		parentMeta.Nlink++
	}
	meta := InodeMeta{
		Inode:      inode,
		Parent:     parent,
		Type:       typ,
		Mode:       opts.Mode,
		UID:        opts.UID,
		GID:        opts.GID,
		AtimeNsec:  now,
		MtimeNsec:  now,
		CtimeNsec:  now,
		ChunkSize:  s.chunkSize,
		HomeSlot:   homeSlot,
		Epoch:      initialEpoch,
		Nlink:      nlink,
		Generation: defaultGeneration,
	}
	parentMeta.MtimeNsec = now
	parentMeta.CtimeNsec = now
	home := Home{HomeSlot: homeSlot, State: HomeStateActive, Epoch: initialEpoch}
	entry := DirEntry{Inode: inode, Type: typ}
	elems, err := putElems(
		fskeys.InodeKey(inode), meta,
		fskeys.HomeKey(inode), home,
		fskeys.DirEntryKey(parent, name), entry,
		fskeys.InodeKey(parent), parentMeta,
		fskeys.DirVersionKey(parent), unixNsecVersion(now),
	)
	if err != nil {
		return CreateResult{}, err
	}
	var fh uint64
	if len(opts.ClientID) > 0 {
		fh, err = s.allocID()
		if err != nil {
			return CreateResult{}, err
		}
		elem, err := s.refPutElem(inode, opts.ClientID, fh)
		if err != nil {
			return CreateResult{}, err
		}
		elems = append(elems, elem)
	}
	readKeys := [][]byte{
		fskeys.InodeKey(parent),
		fskeys.DirEntryKey(parent, name),
		fskeys.InodeKey(inode),
	}
	elems, readKeys, err = s.appendUsageUpdate(ctx, ts, elems, readKeys, usageDelta{filesAdd: 1})
	if err != nil {
		return CreateResult{}, err
	}
	err = s.dispatchTxn(ctx, ts, elems, readKeys)
	if err != nil {
		return CreateResult{}, err
	}
	return CreateResult{Inode: inode, FH: fh, Stat: meta.Stat()}, nil
}

//nolint:cyclop,nestif // unlink/rmdir intentionally share errno and GC decision flow.
func (s *Service) unlink(ctx context.Context, parent uint64, name []byte, directory bool) error {
	if err := validateName(name); err != nil {
		return err
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return err
	}
	parentMeta, err := s.inodeAt(ctx, parent, ts)
	if err != nil {
		return err
	}
	if parentMeta.Type != TypeDirectory {
		return ErrNotDir
	}
	entry, err := s.dirEntryAt(ctx, parent, name, ts)
	if err != nil {
		return err
	}
	meta, err := s.inodeAt(ctx, entry.Inode, ts)
	if err != nil {
		return err
	}
	if directory {
		if meta.Type != TypeDirectory {
			return ErrNotDir
		}
		if err := s.ensureDirectoryEmpty(ctx, entry.Inode, ts); err != nil {
			return err
		}
	} else if meta.Type == TypeDirectory {
		return ErrIsDir
	}
	now := s.now().UnixNano()
	parentMeta.MtimeNsec = now
	parentMeta.CtimeNsec = now
	elems := []*kv.Elem[kv.OP]{{Op: kv.Del, Key: fskeys.DirEntryKey(parent, name)}}
	readKeys := [][]byte{fskeys.InodeKey(parent), fskeys.DirEntryKey(parent, name), fskeys.InodeKey(entry.Inode)}
	var delta usageDelta
	var chunkDeletes *chunkDeletePlan
	if directory {
		if parentMeta.Nlink > fileInitialNlink {
			parentMeta.Nlink--
		}
		elems = append(elems,
			&kv.Elem[kv.OP]{Op: kv.Del, Key: fskeys.InodeKey(entry.Inode)},
			&kv.Elem[kv.OP]{Op: kv.Del, Key: fskeys.HomeKey(entry.Inode)},
			&kv.Elem[kv.OP]{Op: kv.Del, Key: fskeys.DirVersionKey(entry.Inode)},
		)
		readKeys = append(readKeys, fskeys.HomeKey(entry.Inode), fskeys.DirVersionKey(entry.Inode))
		delta = delta.merge(usageDelta{filesSub: 1})
	} else {
		fileElems, fileReads, fileDelta, plan, err := s.unlinkFileTxnParts(ctx, ts, meta, now)
		if err != nil {
			return err
		}
		elems = append(elems, fileElems...)
		readKeys = append(readKeys, fileReads...)
		delta = delta.merge(fileDelta)
		chunkDeletes = plan
	}
	putParent, err := putElem(fskeys.InodeKey(parent), parentMeta)
	if err != nil {
		return err
	}
	putVersion, err := putElem(fskeys.DirVersionKey(parent), unixNsecVersion(now))
	if err != nil {
		return err
	}
	elems = append(elems, putParent, putVersion)
	elems, readKeys, err = s.appendUsageUpdate(ctx, ts, elems, readKeys, delta)
	if err != nil {
		return err
	}
	if err := s.dispatchTxn(ctx, ts, elems, readKeys); err != nil {
		return err
	}
	return s.deleteChunkPages(ctx, chunkDeletes)
}

func (s *Service) unlinkFileTxnParts(
	ctx context.Context,
	ts uint64,
	meta *InodeMeta,
	now int64,
) ([]*kv.Elem[kv.OP], [][]byte, usageDelta, *chunkDeletePlan, error) {
	if meta.Nlink > 0 {
		meta.Nlink--
	}
	if meta.Nlink != 0 {
		elems, err := s.putUnlinkedFileMeta(meta, now)
		return elems, nil, usageDelta{}, nil, err
	}
	hasRefs, refReads, err := s.openRefReadKeys(ctx, meta.Inode, ts)
	if err != nil {
		return nil, nil, usageDelta{}, nil, err
	}
	if !hasRefs {
		gcElems, gcReads, delta, plan, err := s.gcFileElems(ctx, meta, ts)
		if err != nil {
			return nil, nil, usageDelta{}, nil, err
		}
		return gcElems, append(refReads, gcReads...), delta, plan, nil
	}
	meta.Orphaned = true
	elems, err := s.putUnlinkedFileMeta(meta, now)
	return elems, refReads, usageDelta{}, nil, err
}

func (s *Service) putUnlinkedFileMeta(
	meta *InodeMeta,
	now int64,
) ([]*kv.Elem[kv.OP], error) {
	if meta.Nlink != 0 {
		meta.Orphaned = false
	}
	meta.CtimeNsec = now
	elem, err := putElem(fskeys.InodeKey(meta.Inode), meta)
	if err != nil {
		return nil, err
	}
	return []*kv.Elem[kv.OP]{elem}, nil
}

func (s *Service) truncateChunkElems(
	ctx context.Context,
	meta *InodeMeta,
	ts uint64,
	size uint64,
	chunkSize uint64,
) ([]*kv.Elem[kv.OP], [][]byte, usageDelta, *chunkDeletePlan, error) {
	if meta.Size == 0 {
		return nil, nil, usageDelta{}, nil, nil
	}
	oldLast := (meta.Size - 1) / chunkSize
	deleteFrom := size / chunkSize
	var elems []*kv.Elem[kv.OP]
	var readKeys [][]byte
	var delta usageDelta
	var plan *chunkDeletePlan
	if size > 0 && size%chunkSize != 0 {
		tailIndex := size / chunkSize
		tailElems, tailReads, tailDelta, err := s.truncateTailChunkElems(ctx, meta, ts, tailIndex, size%chunkSize)
		if err != nil {
			return nil, nil, usageDelta{}, nil, err
		}
		elems = append(elems, tailElems...)
		readKeys = append(readKeys, tailReads...)
		delta = delta.merge(tailDelta)
		deleteFrom = tailIndex + 1
	}
	if deleteFrom <= oldLast {
		deleteElems, deleteReads, deleteDelta, deletePlan, err := s.truncateDeletedChunkElems(ctx, meta, ts, deleteFrom, chunkSize)
		if err != nil {
			return nil, nil, usageDelta{}, nil, err
		}
		elems = append(elems, deleteElems...)
		readKeys = append(readKeys, deleteReads...)
		delta = delta.merge(deleteDelta)
		plan = deletePlan
	}
	return elems, readKeys, delta, plan, nil
}

func (s *Service) truncateTailChunkElems(
	ctx context.Context,
	meta *InodeMeta,
	ts uint64,
	tailIndex uint64,
	tailKeep uint64,
) ([]*kv.Elem[kv.OP], [][]byte, usageDelta, error) {
	keepLen, err := checkedInt(tailKeep)
	if err != nil {
		return nil, nil, usageDelta{}, err
	}
	tailKey := fskeys.ChunkKey(meta.HomeSlot, meta.Inode, tailIndex)
	raw, err := s.store.GetAt(ctx, tailKey, ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, [][]byte{tailKey}, usageDelta{}, nil
		}
		return nil, nil, usageDelta{}, errors.Wrap(err, "filesystem read truncate tail chunk")
	}
	keep := min(len(raw), keepLen)
	tail := trimChunk(raw[:keep])
	delta := byteLenDelta(uint64(len(raw)), uint64(len(tail)))
	if len(tail) == 0 {
		return []*kv.Elem[kv.OP]{{Op: kv.Del, Key: tailKey}}, [][]byte{tailKey}, delta, nil
	}
	return []*kv.Elem[kv.OP]{{Op: kv.Put, Key: tailKey, Value: tail}}, [][]byte{tailKey}, delta, nil
}

func (s *Service) truncateDeletedChunkElems(
	ctx context.Context,
	meta *InodeMeta,
	ts uint64,
	deleteFrom uint64,
	chunkSize uint64,
) ([]*kv.Elem[kv.OP], [][]byte, usageDelta, *chunkDeletePlan, error) {
	start := fskeys.ChunkKey(meta.HomeSlot, meta.Inode, deleteFrom)
	page, err := s.scanExistingChunkPage(ctx, meta.HomeSlot, meta.Inode, ts, start, chunkDeleteTxnPageSize+1)
	if err != nil {
		return nil, nil, usageDelta{}, nil, errors.Wrap(err, "filesystem scan truncate deleted chunks")
	}
	page, plan := splitChunkDeletePage(page, chunkDeletePlan{
		homeSlot:        meta.HomeSlot,
		inode:           meta.Inode,
		chunkSize:       chunkSize,
		protectLiveSize: true,
	})
	elems, readKeys, delta := chunkDeleteTxnParts(page, nil)
	return elems, readKeys, delta, plan, nil
}

func (s *Service) gcFileElems(ctx context.Context, meta *InodeMeta, ts uint64) ([]*kv.Elem[kv.OP], [][]byte, usageDelta, *chunkDeletePlan, error) {
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: fskeys.InodeKey(meta.Inode)},
		{Op: kv.Del, Key: fskeys.HomeKey(meta.Inode)},
		{Op: kv.Del, Key: fskeys.RefFenceKey(meta.Inode)},
	}
	readKeys := [][]byte{fskeys.InodeKey(meta.Inode), fskeys.HomeKey(meta.Inode), fskeys.RefFenceKey(meta.Inode)}
	delta := usageDelta{filesSub: 1}
	page, err := s.scanExistingChunkPage(ctx, meta.HomeSlot, meta.Inode, ts, fskeys.ChunkPrefix(meta.HomeSlot, meta.Inode), chunkDeleteTxnPageSize+1)
	if err != nil {
		return nil, nil, usageDelta{}, nil, errors.Wrap(err, "filesystem scan gc chunks")
	}
	page, plan := splitChunkDeletePage(page, chunkDeletePlan{
		homeSlot:  meta.HomeSlot,
		inode:     meta.Inode,
		chunkSize: meta.effectiveChunkSize(s.chunkSize),
	})
	deleteElems, deleteReads, deleteDelta := chunkDeleteTxnParts(page, nil)
	elems = append(elems, deleteElems...)
	readKeys = append(readKeys, deleteReads...)
	delta = delta.merge(deleteDelta)
	return elems, readKeys, delta, plan, nil
}

func (s *Service) scanExistingChunkPage(
	ctx context.Context,
	homeSlot uint64,
	inode uint64,
	ts uint64,
	start []byte,
	limit int,
) ([]*store.KVPair, error) {
	prefix := fskeys.ChunkPrefix(homeSlot, inode)
	if bytes.Compare(start, prefix) < 0 || !bytes.HasPrefix(start, prefix) {
		start = prefix
	}
	end := prefixEnd(prefix)
	if end != nil && bytes.Compare(start, end) >= 0 {
		return nil, nil
	}
	page, err := s.store.ScanAt(ctx, start, end, limit, ts)
	if err != nil {
		return nil, errors.Wrap(err, "filesystem scan existing chunk page")
	}
	return page, nil
}

func splitChunkDeletePage(page []*store.KVPair, plan chunkDeletePlan) ([]*store.KVPair, *chunkDeletePlan) {
	if len(page) <= chunkDeleteTxnPageSize {
		return page, nil
	}
	page = page[:chunkDeleteTxnPageSize]
	plan.start = scanCursorAfter(page[len(page)-1].Key)
	return page, &plan
}

func chunkDeleteTxnParts(page []*store.KVPair, readKeys [][]byte) ([]*kv.Elem[kv.OP], [][]byte, usageDelta) {
	elems := make([]*kv.Elem[kv.OP], 0, len(page))
	delta := usageDelta{}
	for _, pair := range page {
		delta = delta.merge(usageDelta{bytesSub: uint64(len(pair.Value))})
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: pair.Key})
		readKeys = append(readKeys, pair.Key)
	}
	return elems, readKeys, delta
}

func (s *Service) deleteChunkPages(ctx context.Context, plan *chunkDeletePlan) error {
	if plan == nil {
		return nil
	}
	start := append([]byte(nil), plan.start...)
	writeConflictRetries := 0
	for {
		next, done, writeConflict, err := s.deleteChunkPage(ctx, plan, start)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if !writeConflict {
			writeConflictRetries = 0
		} else {
			writeConflictRetries++
			writeConflictRetries, err = handleChunkDeleteWriteConflict(ctx, plan, writeConflictRetries)
			if err != nil {
				return err
			}
		}
		start = next
	}
}

func handleChunkDeleteWriteConflict(ctx context.Context, plan *chunkDeletePlan, retries int) (int, error) {
	if retries < chunkDeleteWriteConflictRetries {
		return retries, nil
	}
	if plan.protectLiveSize {
		return retries, errors.Wrap(store.ErrWriteConflict, "filesystem chunk delete retry limit reached")
	}
	if err := waitChunkDeleteRetry(ctx); err != nil {
		return 0, err
	}
	return 0, nil
}

func waitChunkDeleteRetry(ctx context.Context) error {
	timer := time.NewTimer(chunkDeleteWriteConflictBackoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "filesystem chunk delete retry wait")
	case <-timer.C:
		return nil
	}
}

func (s *Service) deleteChunksPastCurrentEOF(ctx context.Context, meta *InodeMeta) error {
	chunkSize := meta.effectiveChunkSize(s.chunkSize)
	start := fskeys.ChunkKey(meta.HomeSlot, meta.Inode, firstChunkIndexPastSize(meta.Size, chunkSize))
	return s.deleteChunkPages(ctx, &chunkDeletePlan{
		homeSlot:        meta.HomeSlot,
		inode:           meta.Inode,
		chunkSize:       chunkSize,
		start:           start,
		protectLiveSize: true,
	})
}

func (s *Service) cleanupNonShrinkingSizeChange(
	ctx context.Context,
	inode uint64,
	size uint64,
	meta *InodeMeta,
	ts uint64,
) (uint64, *InodeMeta, bool, error) {
	if size < meta.Size {
		return ts, meta, false, nil
	}
	if err := s.deleteChunksPastCurrentEOF(ctx, meta); err != nil {
		return 0, nil, false, err
	}
	refreshedTS, err := s.readTS(ctx)
	if err != nil {
		return 0, nil, false, err
	}
	refreshedMeta, err := s.inodeAt(ctx, inode, refreshedTS)
	if err != nil {
		return 0, nil, false, err
	}
	if refreshedMeta.Type == TypeDirectory {
		return 0, nil, false, ErrIsDir
	}
	return refreshedTS, refreshedMeta, true, nil
}

func (s *Service) deleteChunkPage(ctx context.Context, plan *chunkDeletePlan, start []byte) ([]byte, bool, bool, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return nil, false, false, err
	}
	readKeys, adjustedStart, err := s.chunkDeletePlanReadKeys(ctx, plan, ts, start)
	if err != nil {
		return nil, false, false, err
	}
	page, err := s.scanExistingChunkPage(ctx, plan.homeSlot, plan.inode, ts, adjustedStart, chunkDeleteTxnPageSize+1)
	if err != nil {
		return nil, false, false, errors.Wrap(err, "filesystem scan chunk delete page")
	}
	if len(page) == 0 {
		return nil, true, false, nil
	}
	page, nextPlan := splitChunkDeletePage(page, *plan)
	elems, pageReads, delta := chunkDeleteTxnParts(page, readKeys)
	elems, pageReads, err = s.appendUsageUpdate(ctx, ts, elems, pageReads, delta)
	if err != nil {
		return nil, false, false, err
	}
	if err := s.dispatchTxn(ctx, ts, elems, pageReads); err != nil {
		if errors.Is(err, store.ErrWriteConflict) {
			return adjustedStart, false, true, nil
		}
		return nil, false, false, err
	}
	if nextPlan == nil {
		return nil, true, false, nil
	}
	return nextPlan.start, false, false, nil
}

func (s *Service) chunkDeletePlanReadKeys(
	ctx context.Context,
	plan *chunkDeletePlan,
	ts uint64,
	start []byte,
) ([][]byte, []byte, error) {
	if !plan.protectLiveSize {
		return nil, start, nil
	}
	inodeKey := fskeys.InodeKey(plan.inode)
	meta, err := s.inodeAt(ctx, plan.inode, ts)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, start, nil
		}
		return nil, nil, err
	}
	chunkSize := meta.effectiveChunkSize(plan.chunkSize)
	protectStart := fskeys.ChunkKey(plan.homeSlot, plan.inode, firstChunkIndexPastSize(meta.Size, chunkSize))
	if bytes.Compare(start, protectStart) < 0 {
		start = protectStart
	}
	return [][]byte{inodeKey}, start, nil
}

func firstChunkIndexPastSize(size uint64, chunkSize uint64) uint64 {
	if size == 0 {
		return 0
	}
	return (size-1)/chunkSize + 1
}

func (s *Service) appendDirectoryEntries(
	ctx context.Context,
	inode uint64,
	state *readdirCookie,
	limit int,
	result *ReaddirResult,
) error {
	prefix := fskeys.DirEntryPrefix(inode)
	start := prefix
	if len(state.LastName) > 0 {
		start = append(fskeys.DirEntryKey(inode, state.LastName), 0)
	}
	end := prefixEnd(prefix)
	pairs, err := s.store.ScanAt(ctx, start, end, limit, state.ReadTS)
	if err != nil {
		return errors.Wrap(err, "filesystem scan directory entries")
	}
	for _, pair := range pairs {
		name, ok := fskeys.DirEntryNameFromKey(inode, pair.Key)
		if !ok {
			continue
		}
		entry, err := decodeJSON[DirEntry](pair.Value)
		if err != nil {
			return err
		}
		result.Entries = append(result.Entries, Dirent{Name: name, Inode: entry.Inode, Type: entry.Type})
		state.LastName = name
	}
	return nil
}

func (s *Service) readdirState(ctx context.Context, cookie string) (readdirCookie, error) {
	if cookie == "" {
		ts, err := s.readTS(ctx)
		return readdirCookie{ReadTS: ts}, err
	}
	raw, err := base64.RawURLEncoding.DecodeString(cookie)
	if err != nil {
		return readdirCookie{}, ErrInvalid
	}
	var state readdirCookie
	if err := json.Unmarshal(raw, &state); err != nil {
		return readdirCookie{}, ErrInvalid
	}
	if state.ReadTS == 0 || state.Dot > directoryInitialNlink {
		return readdirCookie{}, ErrInvalid
	}
	return state, nil
}

type readdirCookie struct {
	ReadTS   uint64 `json:"read_ts"`
	Dot      uint32 `json:"dot"`
	LastName []byte `json:"last_name,omitempty"`
}

func encodeReaddirCookie(state readdirCookie) (string, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return "", errors.Wrap(err, "filesystem encode readdir cookie")
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (s *Service) inodeAt(ctx context.Context, inode uint64, ts uint64) (*InodeMeta, error) {
	raw, err := s.store.GetAt(ctx, fskeys.InodeKey(inode), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, ErrNotFound
		}
		return nil, errors.Wrap(err, "filesystem read inode")
	}
	meta, err := decodeJSON[InodeMeta](raw)
	if err != nil {
		return nil, err
	}
	return &meta, nil
}

func (s *Service) dirEntryAt(ctx context.Context, parent uint64, name []byte, ts uint64) (DirEntry, error) {
	raw, err := s.store.GetAt(ctx, fskeys.DirEntryKey(parent, name), ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return DirEntry{}, ErrNotFound
		}
		return DirEntry{}, errors.Wrap(err, "filesystem read dir entry")
	}
	return decodeJSON[DirEntry](raw)
}

func (s *Service) ensureDirectoryEmpty(ctx context.Context, inode uint64, ts uint64) error {
	pairs, err := s.store.ScanAt(ctx, fskeys.DirEntryPrefix(inode), prefixEnd(fskeys.DirEntryPrefix(inode)), 1, ts)
	if err != nil {
		return errors.Wrap(err, "filesystem scan directory emptiness")
	}
	if len(pairs) > 0 {
		return ErrNotEmpty
	}
	return nil
}

func (s *Service) openRefReadKeys(ctx context.Context, inode uint64, ts uint64) (bool, [][]byte, error) {
	prefix := fskeys.RefPrefix(inode)
	pairs, err := s.store.ScanAt(ctx, prefix, prefixEnd(prefix), 1, ts)
	if err != nil {
		return false, nil, errors.Wrap(err, "filesystem scan open handle refs")
	}
	readKeys := make([][]byte, 0, len(pairs))
	for _, pair := range pairs {
		readKeys = append(readKeys, pair.Key)
	}
	return len(pairs) > 0, readKeys, nil
}

func (s *Service) hasOtherOpenRefs(ctx context.Context, inode uint64, ts uint64, current []byte) (bool, [][]byte, error) {
	prefix := fskeys.RefPrefix(inode)
	pairs, err := s.store.ScanAt(ctx, prefix, prefixEnd(prefix), openRefScanLimit, ts)
	if err != nil {
		return false, nil, errors.Wrap(err, "filesystem scan open handle refs")
	}
	readKeys := make([][]byte, 0, len(pairs))
	for _, pair := range pairs {
		readKeys = append(readKeys, pair.Key)
		if !bytes.Equal(pair.Key, current) {
			return true, readKeys, nil
		}
	}
	return false, readKeys, nil
}

func (s *Service) allocateInode(ctx context.Context, ts uint64) (uint64, error) {
	for range randomRetryLimit {
		inode, err := s.allocID()
		if err != nil {
			return 0, err
		}
		if inode == 0 || inode == RootInode {
			continue
		}
		if _, err := s.inodeAt(ctx, inode, ts); errors.Is(err, ErrNotFound) {
			return inode, nil
		} else if err != nil {
			return 0, err
		}
	}
	return 0, ErrInodeCollisionLimit
}

func (s *Service) refPutElem(inode uint64, clientID []byte, fh uint64) (*kv.Elem[kv.OP], error) {
	now := s.now()
	value := OpenHandleLease{
		Inode:       inode,
		ClientID:    append([]byte(nil), clientID...),
		FH:          fh,
		CreatedNsec: now.UnixNano(),
		ExpiresNsec: s.openHandleExpiresNsec(now),
	}
	return putElem(fskeys.RefKey(inode, clientID, fh), value)
}

func (s *Service) refFenceElem(inode uint64) (*kv.Elem[kv.OP], error) {
	return putElem(fskeys.RefFenceKey(inode), unixNsecVersion(s.now().UnixNano()))
}

func (s *Service) openHandleLeaseAt(
	ctx context.Context,
	refKey []byte,
	ts uint64,
) (OpenHandleLease, bool, error) {
	raw, err := s.store.GetAt(ctx, refKey, ts)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return OpenHandleLease{}, false, nil
		}
		return OpenHandleLease{}, false, errors.Wrap(err, "filesystem read open handle lease")
	}
	lease, err := decodeJSON[OpenHandleLease](raw)
	return lease, err == nil, err
}

func (s *Service) openHandleExpiresNsec(now time.Time) int64 {
	if s.openLeaseTTL == 0 {
		return 0
	}
	return now.Add(s.openLeaseTTL).UnixNano()
}

func (s *Service) readTS(ctx context.Context) (uint64, error) {
	if reader, ok := s.dispatch.(linearizableReader); ok {
		if _, err := reader.LinearizableRead(ctx); err != nil {
			return 0, errors.Wrap(err, "filesystem linearizable read")
		}
	}
	return s.store.LastCommitTS(), nil
}

func (s *Service) dispatchTxn(
	ctx context.Context,
	startTS uint64,
	elems []*kv.Elem[kv.OP],
	readKeys [][]byte,
) error {
	_, err := s.dispatch.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems:    elems,
		IsTxn:    true,
		StartTS:  startTS,
		ReadKeys: readKeys,
	})
	return errors.Wrap(err, "filesystem dispatch txn")
}

func validateName(name []byte) error {
	switch {
	case len(name) == 0:
		return ErrInvalid
	case bytes.Equal(name, []byte(".")), bytes.Equal(name, []byte("..")):
		return ErrInvalid
	case bytes.ContainsAny(name, "/\x00"):
		return ErrInvalid
	default:
		return nil
	}
}

func (m InodeMeta) effectiveChunkSize(fallback uint64) uint64 {
	if m.ChunkSize > 0 {
		return m.ChunkSize
	}
	return fallback
}

func boundedEnd(offset uint64, size uint64, fileSize uint64) (uint64, error) {
	end, overflow := addUint64(offset, size)
	if overflow {
		return 0, ErrInvalid
	}
	if end > fileSize {
		end = fileSize
	}
	return end, nil
}

func addUint64(a uint64, b uint64) (uint64, bool) {
	if math.MaxUint64-a < b {
		return 0, true
	}
	return a + b, false
}

func checkedInt(v uint64) (int, error) {
	if v > uint64(math.MaxInt) {
		return 0, ErrInvalid
	}
	return int(v), nil //nolint:gosec // v is checked against math.MaxInt immediately above.
}

func unixNsecVersion(v int64) uint64 {
	return uint64(v) //nolint:gosec // Unix nanoseconds from time.Now are non-negative for supported runtime dates.
}

func trimChunk(chunk []byte) []byte {
	last := len(chunk)
	for last > 0 && chunk[last-1] == 0 {
		last--
	}
	return append([]byte(nil), chunk[:last]...)
}

func putElems(args ...any) ([]*kv.Elem[kv.OP], error) {
	if len(args)%keyValuePairArity != 0 {
		return nil, ErrInvalid
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(args)/keyValuePairArity)
	for i := 0; i < len(args); i += keyValuePairArity {
		key, ok := args[i].([]byte)
		if !ok {
			return nil, ErrInvalid
		}
		elem, err := putElem(key, args[i+1])
		if err != nil {
			return nil, err
		}
		elems = append(elems, elem)
	}
	return elems, nil
}

func putElem(key []byte, value any) (*kv.Elem[kv.OP], error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, errors.Wrap(err, "filesystem encode value")
	}
	return &kv.Elem[kv.OP]{Op: kv.Put, Key: append([]byte(nil), key...), Value: raw}, nil
}

func decodeJSON[T any](raw []byte) (T, error) {
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, errors.Wrap(err, "filesystem decode value")
	}
	return out, nil
}

func prefixEnd(prefix []byte) []byte {
	out := append([]byte(nil), prefix...)
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] != math.MaxUint8 {
			out[i]++
			return out[:i+1]
		}
	}
	return nil
}

func randomNonZeroUint64() (uint64, error) {
	var buf [randomUint64Bytes]byte
	for range randomRetryLimit {
		if _, err := rand.Read(buf[:]); err != nil {
			return 0, errors.Wrap(err, "filesystem random id")
		}
		v := binary.BigEndian.Uint64(buf[:])
		if v != 0 {
			return v, nil
		}
	}
	return 0, ErrInodeCollisionLimit
}
