//go:build !unix

package etcd

import (
	"os"

	"github.com/cockroachdb/errors"
)

var errExternalSnapshotInputSymlink = errors.New("external snapshot input path is a symlink")

func openExternalSnapshotInput(path string) (*os.File, error) {
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.WithStack(errExternalSnapshotInputSymlink)
	}
	f, err := os.Open(path) //nolint:gosec // operator-supplied restore artifact path
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return f, nil
}

func isExternalSnapshotInputSymlink(err error) bool {
	return errors.Is(err, errExternalSnapshotInputSymlink)
}
