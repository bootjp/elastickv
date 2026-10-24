package fuseadapter

import (
	"math"
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

func TestToStatus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		errno syscall.Errno
		want  fuse.Status
	}{
		{"zero is OK", 0, fuse.OK},
		{"ENOENT passes through", syscall.ENOENT, fuse.ENOENT},
		{"EOPNOTSUPP passes through", syscall.EOPNOTSUPP, fuse.Status(syscall.EOPNOTSUPP)},
		{"largest int32 passes through", syscall.Errno(math.MaxInt32), fuse.Status(math.MaxInt32)},
		{"above int32 maps to EIO", syscall.Errno(uint64(math.MaxInt32) + 1), fuse.EIO},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, toStatus(tc.errno))
		})
	}
}
