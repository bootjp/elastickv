//go:build unix

package main

import (
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
)

func openNoFollowNonblocking(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0) //nolint:gosec // operator-supplied archive input path
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return os.NewFile(uintptr(fd), path), nil
}

func isNoFollowSymlink(err error) bool {
	return errors.Is(err, syscall.ELOOP)
}
