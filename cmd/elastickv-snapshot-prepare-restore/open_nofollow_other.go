//go:build !unix

package main

import (
	"os"

	"github.com/cockroachdb/errors"
)

var errNoFollowSymlink = errors.New("snapshot restore input path is a symlink")

func openNoFollowNonblocking(path string) (*os.File, error) {
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.WithStack(errNoFollowSymlink)
	}
	f, err := os.Open(path) //nolint:gosec // operator-supplied restore artifact path
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return f, nil
}

func isNoFollowSymlink(err error) bool {
	return errors.Is(err, errNoFollowSymlink)
}
