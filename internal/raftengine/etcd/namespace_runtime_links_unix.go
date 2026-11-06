//go:build linux || darwin

package etcd

import (
	"os"
	"syscall"

	"github.com/cockroachdb/errors"
)

func namespacePreallocationUnlinked(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return errors.Wrap(ErrOfflineBaseline, "runtime WAL preallocation must have exactly one link")
	}
	return nil
}
