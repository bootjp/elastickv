package main

import (
	"os"

	"github.com/bootjp/elastickv/internal/pathsafety"
	"github.com/cockroachdb/errors"
)

func openNoFollowNonblocking(path string) (*os.File, error) {
	f, err := pathsafety.OpenNoFollowRead(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return f, nil
}

func isNoFollowSymlink(err error) bool {
	return pathsafety.IsSymlink(err)
}
