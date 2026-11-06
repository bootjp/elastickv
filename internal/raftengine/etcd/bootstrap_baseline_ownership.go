package etcd

import (
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/storage/wal"
)

type baselineOfflineOwnership struct {
	mu       sync.Mutex
	root     *os.File
	rootInfo os.FileInfo
	files    []*fileutil.LockedFile
	infos    []os.FileInfo
	closed   bool
	sets     map[string]baselinePinnedWAL
}

type baselinePinnedWAL struct {
	directory os.FileInfo
	names     []string
	files     []os.FileInfo
}

// AcquireOfflineBaselineOwnership creates no files. Directory flock excludes
// current engine startup across WAL exchange; retained segment locks also reject
// a running legacy engine. The caller must separately own offline Store access.
func AcquireOfflineBaselineOwnership(dir string) (io.Closer, error) {
	root, err := baselineLockDirectory(dir)
	if err != nil {
		return nil, err
	}
	info, err := root.Stat()
	if err != nil {
		_ = root.Close()
		return nil, errors.WithStack(err)
	}
	o := &baselineOfflineOwnership{root: root, rootInfo: info, sets: make(map[string]baselinePinnedWAL)}
	if err := o.retainWAL(filepath.Join(dir, walDirName)); err != nil {
		_ = o.Close()
		return nil, err
	}
	return o, nil
}

func (o *baselineOfflineOwnership) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil
	}
	o.closed = true
	var err error
	for _, f := range o.files {
		err = errors.CombineErrors(err, f.Close())
	}
	return errors.WithStack(errors.CombineErrors(err, o.root.Close()))
}

func (o *baselineOfflineOwnership) retainWAL(dir string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return errors.Wrap(ErrOfflineBaseline, "offline ownership closed")
	}
	if pinned, ok := o.sets[dir]; ok {
		return verifyBaselinePinnedWAL(dir, pinned)
	}
	names, err := baselineWALNames(dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := o.retainFile(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	pinned, err := o.pinBaselineWALSet(dir, names)
	if err != nil {
		return err
	}
	o.sets[dir] = pinned
	return nil
}

func (o *baselineOfflineOwnership) pinBaselineWALSet(dir string, names []string) (baselinePinnedWAL, error) {
	var zero baselinePinnedWAL
	after, err := baselineWALNames(dir)
	if err != nil {
		return zero, err
	}
	if len(after) != len(names) {
		return zero, errors.Wrap(ErrOfflineBaseline, "WAL segment set changed during lock acquisition")
	}
	for i, name := range names {
		if after[i] != name {
			return zero, errors.Wrap(ErrOfflineBaseline, "WAL segment set changed during lock acquisition")
		}
	}
	pinned := baselinePinnedWAL{names: names}
	pinned.directory, err = os.Lstat(dir)
	if err != nil {
		return zero, errors.WithStack(err)
	}
	for _, name := range names {
		info, err := os.Lstat(filepath.Join(dir, name))
		if err != nil {
			return zero, errors.WithStack(err)
		}
		if !o.hasPinnedFile(info) {
			return zero, errors.Wrap(ErrOfflineBaseline, "WAL file changed after lock acquisition")
		}
		pinned.files = append(pinned.files, info)
	}
	return pinned, nil
}

func (o *baselineOfflineOwnership) hasPinnedFile(info os.FileInfo) bool {
	for _, pinned := range o.infos {
		if os.SameFile(info, pinned) {
			return true
		}
	}
	return false
}

func verifyBaselinePinnedWAL(dir string, pinned baselinePinnedWAL) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	if !os.SameFile(info, pinned.directory) {
		return errors.Wrap(ErrOfflineBaseline, "WAL directory replaced while owned")
	}
	names, err := baselineWALNames(dir)
	if err != nil {
		return err
	}
	if len(names) != len(pinned.names) {
		return errors.Wrap(ErrOfflineBaseline, "WAL segment set changed while owned")
	}
	for i, name := range names {
		info, err := os.Lstat(filepath.Join(dir, name))
		if err != nil {
			return errors.WithStack(err)
		}
		if name != pinned.names[i] || !os.SameFile(info, pinned.files[i]) {
			return errors.Wrap(ErrOfflineBaseline, "WAL segment replaced while owned")
		}
	}
	return nil
}

func (o *baselineOfflineOwnership) exchangeWAL(a, b string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return errors.Wrap(ErrOfflineBaseline, "offline ownership closed")
	}
	if err := verifyBaselinePinnedWAL(a, o.sets[a]); err != nil {
		return err
	}
	if err := verifyBaselinePinnedWAL(b, o.sets[b]); err != nil {
		return err
	}
	if err := baselineAtomicExchange(a, b); err != nil {
		return err
	}
	o.sets[a], o.sets[b] = o.sets[b], o.sets[a]
	return nil
}

func (o *baselineOfflineOwnership) retainFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, pinned := range o.infos {
		if os.SameFile(info, pinned) {
			return nil
		}
	}
	f, err := fileutil.TryLockFile(path, os.O_RDWR, 0)
	if err != nil {
		return errors.Wrap(err, "offline WAL ownership unavailable; server may be running")
	}
	pinned, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return errors.WithStack(err)
	}
	if !os.SameFile(info, pinned) {
		_ = f.Close()
		return errors.Wrap(ErrOfflineBaseline, "WAL file replaced while locking")
	}
	o.files, o.infos = append(o.files, f), append(o.infos, pinned)
	return nil
}

func verifyBaselineOwnership(options OfflineBaselineOptions) (*baselineOfflineOwnership, error) {
	o, ok := options.Ownership.(*baselineOfflineOwnership)
	if !ok {
		return nil, errors.Wrap(ErrOfflineBaseline, "native offline ownership token required")
	}
	info, err := os.Lstat(options.DataDir)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !os.SameFile(info, o.rootInfo) {
		return nil, errors.Wrap(ErrOfflineBaseline, "data directory identity changed")
	}
	if err := o.retainWAL(filepath.Join(options.DataDir, walDirName)); err != nil {
		return nil, err
	}
	return o, nil
}

func acquireBaselineEngineGuard(dir string) (io.Closer, error) {
	if _, err := os.Lstat(dir); os.IsNotExist(err) {
		return nil, nil
	}
	return baselineLockDirectory(dir)
}

type baselineEngineLease struct {
	guard       io.Closer
	transferred bool
}

func (g *baselineEngineLease) Close() error {
	if g.guard == nil {
		return nil
	}
	return errors.WithStack(g.guard.Close())
}
func (g *baselineEngineLease) abandon() {
	if !g.transferred {
		_ = g.Close()
	}
}

func prepareBaselineOwnedOpen(cfg OpenConfig) (preparedOpenState, *baselineEngineLease, error) {
	lease := &baselineEngineLease{}
	var guard io.Closer
	var err error
	if cfg.namespaceOwnership != nil {
		guard, err = claimNamespaceRecoveryOwnership(cfg)
	} else {
		guard, err = acquireBaselineEngineGuard(cfg.DataDir)
	}
	if err != nil {
		return preparedOpenState{}, nil, err
	}
	lease.guard = guard
	if err := validateBaselineStartupSource(cfg); err != nil {
		_ = lease.Close()
		return preparedOpenState{}, nil, err
	}
	prepared, err := prepareOpenState(cfg)
	if err != nil {
		_ = lease.Close()
		return preparedOpenState{}, nil, err
	}
	if lease.guard == nil {
		lease.guard, err = acquireBaselineEngineGuard(cfg.DataDir)
		if err != nil {
			_ = closePersist(prepared.disk.Persist)
			return preparedOpenState{}, nil, err
		}
	}
	return prepared, lease, nil
}

// A preparation-bound identity is created only by verified offline migration,
// never by the backend's historical single-peer auto-bootstrap path.
func validateBaselineStartupSource(cfg OpenConfig) error {
	if cfg.NativeIdentity == nil {
		return nil
	}
	if wal.Exist(filepath.Join(cfg.DataDir, walDirName)) {
		return nil
	}
	if cfg.Bootstrap && cfg.NativeIdentity.PreparationDigest == (raftbootstrap.Digest{}) {
		return nil
	}
	return errors.Wrap(ErrWALIdentity, "protected recovery requires an existing WAL; preparation cannot bootstrap")
}
