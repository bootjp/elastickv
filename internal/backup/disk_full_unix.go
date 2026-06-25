//go:build unix

package backup

import (
	"errors"
	"syscall"
)

// isDiskFullError reports whether err is a POSIX ENOSPC anywhere in
// the wrap chain. os.File.Write surfaces ENOSPC inside an
// os.PathError which errors.Is unwraps natively.
func isDiskFullError(err error) bool {
	return errors.Is(err, syscall.ENOSPC)
}
