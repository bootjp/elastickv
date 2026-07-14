//go:build !(aix || darwin || linux)

package etcd

import "math"

func snapshotSpoolAvailableBytesFS(string) (int64, error) {
	return math.MaxInt64, nil
}
