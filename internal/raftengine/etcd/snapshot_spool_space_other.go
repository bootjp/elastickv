//go:build !(aix || darwin || dragonfly || freebsd || linux || netbsd || openbsd || solaris)

package etcd

import "math"

func snapshotSpoolAvailableBytesFS(string) (int64, error) {
	return math.MaxInt64, nil
}
