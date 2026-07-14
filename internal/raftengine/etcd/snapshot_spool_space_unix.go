//go:build aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris

package etcd

import (
	"math"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
)

func snapshotSpoolAvailableBytesFS(path string) (int64, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return 0, errors.WithStack(err)
	}
	if st.Bsize <= 0 {
		return 0, nil
	}
	blockSize := uint64(st.Bsize)
	availableBlocks := st.Bavail
	if availableBlocks > uint64(math.MaxInt64)/blockSize {
		return math.MaxInt64, nil
	}
	availableBytes := availableBlocks * blockSize
	if availableBytes > uint64(math.MaxInt64) {
		return math.MaxInt64, nil
	}
	return int64(availableBytes), nil
}
