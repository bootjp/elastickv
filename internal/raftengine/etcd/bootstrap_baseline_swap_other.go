//go:build !linux && !darwin

package etcd

import (
	"github.com/cockroachdb/errors"
	"os"
)

func baselineLockDirectory(_ string) (*os.File, error) {
	return nil, errors.Wrap(ErrOfflineBaseline, "offline directory ownership unsupported")
}

func baselineAtomicExchange(_, _ string) error {
	return errors.Wrap(ErrOfflineBaseline, "atomic WAL exchange is unsupported on this platform")
}
