package backup

import (
	"os"

	"github.com/bootjp/elastickv/internal/pathsafety"
	"github.com/cockroachdb/errors"
)

func openArchiveRegularForRead(path string) (*os.File, os.FileInfo, error) {
	f, err := pathsafety.OpenNoFollowRead(path)
	if err != nil {
		if pathsafety.IsSymlink(err) {
			return nil, nil, errors.Wrapf(ErrArchiveNonRegular, "%s is a symlink", path)
		}
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
