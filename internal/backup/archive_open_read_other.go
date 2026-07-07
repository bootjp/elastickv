//go:build !unix

package backup

import (
	"os"

	"github.com/cockroachdb/errors"
)

func openArchiveRegularForRead(path string) (*os.File, os.FileInfo, error) {
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, nil, errors.Wrapf(ErrArchiveNonRegular, "%s is a symlink", path)
	}
	f, err := os.Open(path) //nolint:gosec // path came from WalkDir under the verified dump root
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, nil, errors.WithStack(err)
	}
	if !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, nil, errors.Wrapf(ErrArchiveNonRegular, "%s is not a regular file", path)
	}
	if err := refuseArchiveHardLink(info, path); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return f, info, nil
}
