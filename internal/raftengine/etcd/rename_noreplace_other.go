//go:build !linux && !darwin

package etcd

import (
	"os"

	"github.com/cockroachdb/errors"
)

func renameDirNoReplace(oldPath string, newPath string) error {
	if _, err := os.Lstat(newPath); err == nil {
		return os.ErrExist
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Rename(oldPath, newPath)
}

func isRenameNoReplaceExists(err error) bool {
	return errors.Is(err, os.ErrExist)
}
