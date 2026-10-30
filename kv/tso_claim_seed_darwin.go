//go:build darwin

package kv

import (
	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

func publishTSOClaimSeed(directory int, temporary, final string) error {
	return errors.Wrap(unix.RenameatxNp(directory, temporary, directory, final, unix.RENAME_EXCL), "publish claim seed without replacement")
}
