package etcd

import (
	"io"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
)

type namespaceRuntimeOwnership struct {
	*baselineOfflineOwnership
	wal namespaceWALInventory
}

// AcquireNamespaceRecoveryOwnership is runtime-only. It retains the root fence
// and locks existing segments/preallocations without creating or deleting files.
// Unlike the offline baseline token, it cannot authorize migration or signing.
func AcquireNamespaceRecoveryOwnership(dir string) (io.Closer, error) {
	root, err := baselineLockDirectory(dir)
	if err != nil {
		return nil, err
	}
	info, err := root.Stat()
	if err != nil {
		_ = root.Close()
		return nil, errors.WithStack(err)
	}
	o := &namespaceRuntimeOwnership{baselineOfflineOwnership: &baselineOfflineOwnership{root: root, rootInfo: info}}
	if err := o.pin(dir); err != nil {
		_ = o.Close()
		return nil, err
	}
	return o, nil
}

func (o *namespaceRuntimeOwnership) pin(dir string) error {
	walDir := filepath.Join(dir, walDirName)
	inventory, err := inspectNamespaceWAL(walDir)
	if err != nil {
		return err
	}
	for _, file := range inventory.files {
		if err := o.retainFile(filepath.Join(walDir, file.name)); err != nil {
			return err
		}
		if !o.hasPinnedFile(file.info) {
			return errors.Wrap(ErrOfflineBaseline, "runtime WAL file replaced during locking")
		}
	}
	o.wal = inventory
	return o.verify(dir)
}

func (o *namespaceRuntimeOwnership) verify(dir string) error {
	info, err := os.Lstat(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	if !os.SameFile(info, o.rootInfo) {
		return errors.Wrap(ErrOfflineBaseline, "runtime data directory replaced while owned")
	}
	current, err := inspectNamespaceWAL(filepath.Join(dir, walDirName))
	if err != nil {
		return err
	}
	if !os.SameFile(current.directory, o.wal.directory) || len(current.files) != len(o.wal.files) {
		return errors.Wrap(ErrOfflineBaseline, "runtime WAL set changed while owned")
	}
	for i, file := range current.files {
		if file.name != o.wal.files[i].name || !os.SameFile(file.info, o.wal.files[i].info) {
			return errors.Wrap(ErrOfflineBaseline, "runtime WAL file replaced while owned")
		}
	}
	return nil
}

func (o *namespaceRuntimeOwnership) claim(dir string) (io.Closer, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.root == nil {
		return nil, errors.WithStack(ErrOfflineBaseline)
	}
	if err := o.verify(dir); err != nil {
		return nil, err
	}
	for _, file := range o.files {
		if err := file.Close(); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	o.files = nil
	root := o.root
	o.root, o.closed = nil, true
	return &namespaceRootGuard{closer: root}, nil
}
