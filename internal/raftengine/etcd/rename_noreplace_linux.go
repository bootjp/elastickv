//go:build linux

package etcd

import (
	"os"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

func renameDirNoReplace(oldPath string, newPath string) error {
	if err := unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_NOREPLACE); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func isRenameNoReplaceExists(err error) bool {
	return errors.Is(err, unix.EEXIST) || errors.Is(err, os.ErrExist)
}
