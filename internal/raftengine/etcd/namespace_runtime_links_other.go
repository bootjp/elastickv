//go:build !linux && !darwin

package etcd

import (
	"os"

	"github.com/cockroachdb/errors"
)

func namespacePreallocationUnlinked(_ os.FileInfo) error {
	return errors.Wrap(ErrOfflineBaseline, "runtime WAL preallocation link verification unsupported")
}
