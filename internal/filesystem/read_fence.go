package filesystem

import (
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// ErrReadFenceUnavailable marks a failed read barrier while its leader or
// transport was unavailable. No filesystem mutation was submitted by the read.
var ErrReadFenceUnavailable = errors.New("filesystem read fence unavailable")

func markReadFenceError(err error) error {
	if kv.IsTransientReadBarrierError(err) {
		return errors.Join(err, ErrReadFenceUnavailable)
	}
	return err
}
