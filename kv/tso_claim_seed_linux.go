//go:build linux

package kv

import (
	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

func publishTSOClaimSeed(directory int, temporary, final string) error {
	return errors.Wrap(unix.Renameat2(directory, temporary, directory, final, unix.RENAME_NOREPLACE), "publish claim seed without replacement")
}
