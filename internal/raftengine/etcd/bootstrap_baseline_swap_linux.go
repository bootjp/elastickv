//go:build linux

package etcd

import (
	"os"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

func baselineAtomicExchange(a, b string) error {
	return errors.WithStack(unix.Renameat2(unix.AT_FDCWD, a, unix.AT_FDCWD, b, unix.RENAME_EXCHANGE))
}

func baselineLockDirectory(path string) (*os.File, error) {
	if err := baselineRegularDirectory(path); err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, errors.WithStack(err)
	}
	return f, nil
}
