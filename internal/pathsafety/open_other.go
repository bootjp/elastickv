//go:build !unix && !windows

package pathsafety

import (
	"os"

	"github.com/cockroachdb/errors"
)

func OpenNoFollowRead(path string) (*os.File, error) {
	return nil, errors.Wrapf(ErrUnsupported, "%s", path)
}
