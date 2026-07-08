package pathsafety

import "github.com/cockroachdb/errors"

var (
	ErrSymlink     = errors.New("path safety: symlink refused")
	ErrUnsupported = errors.New("path safety: atomic no-follow open unsupported")
)

func IsSymlink(err error) bool {
	return errors.Is(err, ErrSymlink)
}
