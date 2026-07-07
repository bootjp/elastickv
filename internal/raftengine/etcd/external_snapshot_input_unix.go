//go:build unix

package etcd

import (
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
)

func openExternalSnapshotInput(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0) //nolint:gosec // operator-supplied restore artifact path
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return os.NewFile(uintptr(fd), path), nil
}

func isExternalSnapshotInputSymlink(err error) bool {
	return errors.Is(err, syscall.ELOOP)
}
