//go:build unix

package backup

import (
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
)

func openArchiveRegularForRead(path string) (*os.File, os.FileInfo, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0) //nolint:gosec // path came from WalkDir under the verified dump root
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, nil, errors.Wrapf(ErrArchiveNonRegular, "%s is a symlink", path)
		}
		return nil, nil, errors.WithStack(err)
	}
	f := os.NewFile(uintptr(fd), path)
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, errors.WithStack(err)
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, nil, errors.Wrapf(ErrArchiveNonRegular, "%s is not a regular file", path)
	}
	if err := refuseArchiveHardLink(info, path); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return f, info, nil
}
