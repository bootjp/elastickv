package etcd

import (
	"os"

	"github.com/bootjp/elastickv/internal/pathsafety"
	"github.com/cockroachdb/errors"
)

func openExternalSnapshotInput(path string) (*os.File, error) {
	f, err := pathsafety.OpenNoFollowRead(path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return f, nil
}

func isExternalSnapshotInputSymlink(err error) bool {
	return pathsafety.IsSymlink(err)
}
