//go:build darwin

package etcd

import (
	"os"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

func baselineAtomicExchange(a, b string) error {
	return errors.WithStack(unix.RenamexNp(a, b, unix.RENAME_SWAP))
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
