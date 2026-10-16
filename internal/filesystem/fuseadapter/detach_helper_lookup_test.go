package fuseadapter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestDetachFUSEMountBoundsHelperLookup stands in a helper lookup that never
// returns, as exec.LookPath does when a PATH directory sits on a wedged FUSE
// or network filesystem. The lookup must be bounded like the helper itself,
// so the kernel fallback is still attempted inside the caller's budget (here
// on a plain directory, so it fails with its own error, not the deadline's).
func TestDetachFUSEMountBoundsHelperLookup(t *testing.T) {
	mountPoint := t.TempDir()
	useFakeDetachMountTable(t, fuseTableLine(mountPoint))
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	old := lookupMountHelperFn
	lookupMountHelperFn = func(string) (string, error) {
		<-block
		return "", nil
	}
	t.Cleanup(func() { lookupMountHelperFn = old })

	budget := 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	start := time.Now()
	method, err := detachFUSEMount(ctx, mountPoint, fuseTypeFor(filesystemName))
	elapsed := time.Since(start)
	require.Error(t, err)
	require.Empty(t, method)
	require.NotErrorIs(t, err, context.DeadlineExceeded, "the hung lookups must not pre-empt the kernel fallback")
	require.ErrorContains(t, err, lazyUnmountMethod)
	require.Less(t, elapsed, budget, "the fallback must run inside the caller's budget")
}
