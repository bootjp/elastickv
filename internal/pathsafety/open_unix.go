//go:build unix

package pathsafety

import (
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
)

func OpenNoFollowRead(path string) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0) //nolint:gosec // caller validates the operator-supplied path after opening
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, errors.Wrapf(ErrSymlink, "%s", path)
		}
		return nil, errors.WithStack(err)
	}
	f := os.NewFile(uintptr(fd), path)
	if f == nil {
		_ = syscall.Close(fd)
		return nil, errors.New("path safety: os.NewFile returned nil")
	}
	return f, nil
}
