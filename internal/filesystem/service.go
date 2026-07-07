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

	bytesPerMiB               uint64 = 1 << 20
	DefaultChunkSize                 = 4 * bytesPerMiB
	defaultGeneration         uint64 = 1
	initialEpoch              uint64 = 1
	directoryInitialNlink     uint32 = 2
	fileInitialNlink          uint32 = 1
	rootParentInode           uint64 = RootInode
	randomRetryLimit                 = 16
	randomUint64Bytes                = 8
	keyValuePairArity                = 2
	maxReadSize                      = 64 * bytesPerMiB
	maxScanPageSize                  = 1024
	defaultOpenHandleLeaseTTL        = 5 * time.Minute
	defaultLeaseReaperLimit          = 256
	statFSScanPageSize               = 512
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
	)
	if err != nil {
		return err
	}
	_, err = s.dispatch.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		Elems:    elems,
		IsTxn:    true,
		StartTS:  ts,
		ReadKeys: [][]byte{fskeys.InodeKey(RootInode)},
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
	if mask.Size {
		if err := s.Truncate(ctx, inode, attrs.Size); err != nil {
			return Stat{}, err
		}
		if !mask.Mode && !mask.UID && !mask.GID && !mask.Atime && !mask.Mtime {
			return s.GetAttr(ctx, inode)
		}
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return Stat{}, err
	}
	meta, err := s.inodeAt(ctx, inode, ts)
	if err != nil {
		return Stat{}, err
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
	}
	meta.CtimeNsec = now
	elem, err := putElem(fskeys.InodeKey(inode), meta)
	if err != nil {
		return Stat{}, err
	}
	if err := s.dispatchTxn(ctx, ts, []*kv.Elem[kv.OP]{elem}, [][]byte{fskeys.InodeKey(inode)}); err != nil {
		return Stat{}, err
	}
	return meta.Stat(), nil
}

func (s *Service) Create(ctx context.Context, parent uint64, name []byte, opts CreateOptions) (CreateResult, error) {
	return s.createNode(ctx, parent, name, TypeFile, opts)
}

func (s *Service) Mkdir(ctx context.Context, parent uint64, name []byte, opts CreateOptions) (CreateResult, error) {
	return s.createNode(ctx, parent, name, TypeDirectory, opts)
}

func (s *Service) Open(ctx context.Context, inode uint64, clientID []byte) (uint64, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return 0, err
	}
	if _, err := s.inodeAt(ctx, inode, ts); err != nil {
		return 0, err
	}
	fh, err := s.allocID()
	if err != nil {
		return 0, err
	}
	elem, err := s.refPutElem(inode, clientID, fh)
	if err != nil {
		return 0, err
	}
	if err := s.dispatchTxn(ctx, ts, []*kv.Elem[kv.OP]{elem}, [][]byte{fskeys.InodeKey(inode)}); err != nil {
		return 0, err
	}
	return fh, nil
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
	for idx := first; idx <= last; idx++ {
		chunkKey := fskeys.ChunkKey(meta.HomeSlot, inode, idx)
		chunk := make([]byte, chunkLen)
		raw, getErr := s.store.GetAt(ctx, chunkKey, ts)
		if getErr != nil && !errors.Is(getErr, store.ErrKeyNotFound) {
			return 0, errors.Wrap(getErr, "filesystem read chunk for write")
		}
		copy(chunk, raw)
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
		dstStart, err := checkedInt(writeStart - chunkStart)
		if err != nil {
			return 0, err
		}
		copy(chunk[dstStart:dstStart+(srcEnd-srcStart)], data[srcStart:srcEnd])
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: chunkKey, Value: trimChunk(chunk)})
		readKeys = append(readKeys, chunkKey)
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
	if err := s.dispatchTxn(ctx, ts, elems, readKeys); err != nil {
		return 0, err
	}
	return len(data), nil
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
	chunkSize := meta.effectiveChunkSize(s.chunkSize)
	elems := make([]*kv.Elem[kv.OP], 0)
	readKeys := [][]byte{fskeys.InodeKey(inode), fskeys.HomeKey(inode)}
	if size < meta.Size {
		truncateElems, truncateReads, err := s.truncateChunkElems(ctx, meta, ts, size, chunkSize)
		if err != nil {
			return err
		}
		elems = append(elems, truncateElems...)
		readKeys = append(readKeys, truncateReads...)
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
	return s.dispatchTxn(ctx, ts, elems, readKeys)
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
	if bytes.Equal(oldName, newName) {
		return nil
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
	if newEntry, getErr := s.dirEntryAt(ctx, oldParent, newName, ts); getErr == nil {
		if newEntry.Type == TypeDirectory {
			return ErrIsDir
		}
	} else if !errors.Is(getErr, ErrNotFound) {
		return getErr
	}
	now := s.now().UnixNano()
	parentMeta.MtimeNsec = now
	parentMeta.CtimeNsec = now
	newEntry := DirEntry{Inode: oldEntry.Inode, Type: oldEntry.Type}
	putNew, err := putElem(fskeys.DirEntryKey(oldParent, newName), newEntry)
	if err != nil {
		return err
	}
	putParent, err := putElem(fskeys.InodeKey(oldParent), parentMeta)
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
		putParent,
		putVersion,
	}
	err = s.dispatchTxn(ctx, ts, elems, [][]byte{
		fskeys.InodeKey(oldParent),
		fskeys.DirEntryKey(oldParent, oldName),
		fskeys.DirEntryKey(oldParent, newName),
	})
	return err
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
	err = s.dispatchTxn(ctx, ts, []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: fskeys.RefKey(inode, clientID, fh)},
	}, [][]byte{fskeys.RefKey(inode, clientID, fh)})
	if err != nil {
		return err
	}
	_, err = s.gcOrphanIfUnreferenced(ctx, inode)
	return err
}

func (s *Service) StatFS(ctx context.Context, _ uint64) (StatFS, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return StatFS{}, err
	}
	usage, err := s.statFSUsage(ctx, ts)
	if err != nil {
		return StatFS{}, err
	}
	return StatFS{
		ChunkSize: s.chunkSize,
		Files:     usage.files,
		FreeFiles: freeAfterUsed(s.maxFiles, usage.files, math.MaxUint64),
		Capacity:  s.capacity,
		Free:      freeAfterUsed(s.capacity, usage.bytes, 0),
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
	readTS, err := s.readTS(ctx)
	if err != nil {
		return LeaseReapStats{}, err
	}
	prefix := fskeys.RefAllPrefix()
	pairs, err := s.store.ScanAt(ctx, prefix, prefixEnd(prefix), limit, readTS)
	if err != nil {
		return LeaseReapStats{}, errors.Wrap(err, "filesystem scan open handle leases")
	}
	var stats LeaseReapStats
	nowNsec := s.now().UnixNano()
	for _, pair := range pairs {
		reaped, gcd, err := s.reapExpiredOpenHandleLease(ctx, pair, nowNsec)
		if err != nil {
			return stats, err
		}
		if reaped {
			stats.ExpiredRefs++
		}
		if gcd {
			stats.OrphanedInodesGCed++
		}
	}
	return stats, nil
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
	if err := s.dispatchTxn(ctx, ts, []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: pair.Key},
	}, [][]byte{pair.Key}); err != nil {
		return false, false, err
	}
	gcd, err := s.gcOrphanIfUnreferenced(ctx, lease.Inode)
	return true, gcd, err
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
	err = s.dispatchTxn(ctx, ts, elems, [][]byte{
		fskeys.InodeKey(parent),
		fskeys.DirEntryKey(parent, name),
		fskeys.InodeKey(inode),
	})
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
	if directory {
		if parentMeta.Nlink > fileInitialNlink {
			parentMeta.Nlink--
		}
		elems = append(elems,
			&kv.Elem[kv.OP]{Op: kv.Del, Key: fskeys.InodeKey(entry.Inode)},
			&kv.Elem[kv.OP]{Op: kv.Del, Key: fskeys.HomeKey(entry.Inode)},
			&kv.Elem[kv.OP]{Op: kv.Del, Key: fskeys.DirVersionKey(entry.Inode)},
		)
	} else {
		if meta.Nlink > 0 {
			meta.Nlink--
		}
		if meta.Nlink == 0 && !s.hasOpenRefs(ctx, meta.Inode, ts) {
			gcElems, gcReads := s.gcFileElems(meta)
			elems = append(elems, gcElems...)
			readKeys = append(readKeys, gcReads...)
		} else {
			meta.Orphaned = meta.Nlink == 0
			meta.CtimeNsec = now
			elem, err := putElem(fskeys.InodeKey(meta.Inode), meta)
			if err != nil {
				return err
			}
			elems = append(elems, elem)
		}
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
	return s.dispatchTxn(ctx, ts, elems, readKeys)
}

func (s *Service) truncateChunkElems(
	ctx context.Context,
	meta *InodeMeta,
	ts uint64,
	size uint64,
	chunkSize uint64,
) ([]*kv.Elem[kv.OP], [][]byte, error) {
	if meta.Size == 0 {
		return nil, nil, nil
	}
	oldLast := (meta.Size - 1) / chunkSize
	deleteFrom := size / chunkSize
	var elems []*kv.Elem[kv.OP]
	var readKeys [][]byte
	if size > 0 && size%chunkSize != 0 {
		tailIndex := size / chunkSize
		tailKeep, err := checkedInt(size % chunkSize)
		if err != nil {
			return nil, nil, err
		}
		tailKey := fskeys.ChunkKey(meta.HomeSlot, meta.Inode, tailIndex)
		raw, err := s.store.GetAt(ctx, tailKey, ts)
		if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
			return nil, nil, errors.Wrap(err, "filesystem read truncate tail chunk")
		}
		tail := make([]byte, tailKeep)
		copy(tail, raw)
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: tailKey, Value: tail})
		readKeys = append(readKeys, tailKey)
		deleteFrom = tailIndex + 1
	}
	for idx := deleteFrom; idx <= oldLast; idx++ {
		key := fskeys.ChunkKey(meta.HomeSlot, meta.Inode, idx)
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
		readKeys = append(readKeys, key)
	}
	return elems, readKeys, nil
}

func (s *Service) gcFileElems(meta *InodeMeta) ([]*kv.Elem[kv.OP], [][]byte) {
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: fskeys.InodeKey(meta.Inode)},
		{Op: kv.Del, Key: fskeys.HomeKey(meta.Inode)},
	}
	readKeys := [][]byte{fskeys.InodeKey(meta.Inode), fskeys.HomeKey(meta.Inode)}
	if meta.Size == 0 {
		return elems, readKeys
	}
	chunkSize := meta.effectiveChunkSize(s.chunkSize)
	last := (meta.Size - 1) / chunkSize
	for idx := uint64(0); idx <= last; idx++ {
		key := fskeys.ChunkKey(meta.HomeSlot, meta.Inode, idx)
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
		readKeys = append(readKeys, key)
	}
	return elems, readKeys
}

func (s *Service) gcOrphanIfUnreferenced(ctx context.Context, inode uint64) (bool, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return false, err
	}
	meta, err := s.inodeAt(ctx, inode, ts)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if !meta.Orphaned || meta.Nlink != 0 || s.hasOpenRefs(ctx, inode, ts) {
		return false, nil
	}
	elems, readKeys := s.gcFileElems(meta)
	if err := s.dispatchTxn(ctx, ts, elems, readKeys); err != nil {
		return false, err
	}
	return true, nil
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

func (s *Service) hasOpenRefs(ctx context.Context, inode uint64, ts uint64) bool {
	pairs, err := s.store.ScanAt(ctx, fskeys.RefPrefix(inode), prefixEnd(fskeys.RefPrefix(inode)), 1, ts)
	return err == nil && len(pairs) > 0
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

func (s *Service) openHandleExpiresNsec(now time.Time) int64 {
	if s.openLeaseTTL == 0 {
		return 0
	}
	return now.Add(s.openLeaseTTL).UnixNano()
}

func (s *Service) readTS(ctx context.Context) (uint64, error) {
	if reader, ok := s.dispatch.(linearizableReader); ok {
		ts, err := reader.LinearizableRead(ctx)
		return ts, errors.Wrap(err, "filesystem linearizable read")
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
