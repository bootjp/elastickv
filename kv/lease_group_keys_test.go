package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// fakeGroupRoutableCoordinator is a minimal Coordinator that also
// implements GroupRoutableCoordinator, mapping each key to a caller-
// supplied group ID. Only the methods LeaseReadGroupKeys exercises are
// implemented; the rest panic to catch unexpected use.
type fakeGroupRoutableCoordinator struct {
	groupByKey map[string]uint64
}

func (f *fakeGroupRoutableCoordinator) EngineGroupIDForKey(key []byte) uint64 {
	return f.groupByKey[string(key)]
}

func (f *fakeGroupRoutableCoordinator) Dispatch(context.Context, *OperationGroup[OP]) (*CoordinateResponse, error) {
	panic("unused")
}
func (f *fakeGroupRoutableCoordinator) IsLeader() bool                     { panic("unused") }
func (f *fakeGroupRoutableCoordinator) VerifyLeader(context.Context) error { panic("unused") }
func (f *fakeGroupRoutableCoordinator) RaftLeader() string                 { panic("unused") }
func (f *fakeGroupRoutableCoordinator) IsLeaderForKey([]byte) bool         { panic("unused") }
func (f *fakeGroupRoutableCoordinator) VerifyLeaderForKey(context.Context, []byte) error {
	panic("unused")
}
func (f *fakeGroupRoutableCoordinator) RaftLeaderForKey([]byte) string { panic("unused") }
func (f *fakeGroupRoutableCoordinator) LinearizableRead(context.Context) (uint64, error) {
	panic("unused")
}
func (f *fakeGroupRoutableCoordinator) Clock() *HLC { panic("unused") }

var _ Coordinator = (*fakeGroupRoutableCoordinator)(nil)
var _ GroupRoutableCoordinator = (*fakeGroupRoutableCoordinator)(nil)

// plainNonRoutable implements Coordinator (via embedding) but NOT
// GroupRoutableCoordinator, so LeaseReadGroupKeys must fall back to
// returning the input unchanged. LeaseReadGroupKeys only type-asserts;
// it never calls a Coordinator method, so the embedded nil interface is
// fine.
type plainNonRoutable struct {
	Coordinator
}

func keys(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func equalKeySlices(t *testing.T, want, got [][]byte) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Truef(t, bytes.Equal(want[i], got[i]),
			"index %d: want %q got %q", i, want[i], got[i])
	}
}

func TestLeaseReadGroupKeys(t *testing.T) {
	t.Parallel()

	t.Run("collapses keys sharing a group to one representative", func(t *testing.T) {
		t.Parallel()
		c := &fakeGroupRoutableCoordinator{groupByKey: map[string]uint64{
			"a": 1, "b": 1, "c": 1,
		}}
		got := LeaseReadGroupKeys(c, keys("a", "b", "c"))
		// All three share group 1: only the first appearance survives.
		equalKeySlices(t, keys("a"), got)
	})

	t.Run("keeps one representative per distinct group in first-seen order", func(t *testing.T) {
		t.Parallel()
		c := &fakeGroupRoutableCoordinator{groupByKey: map[string]uint64{
			"a": 1, "b": 2, "c": 1, "d": 2, "e": 3,
		}}
		got := LeaseReadGroupKeys(c, keys("a", "b", "c", "d", "e"))
		// Groups 1,2,3 first appear at a,b,e respectively.
		equalKeySlices(t, keys("a", "b", "e"), got)
	})

	t.Run("never collapses unroutable keys", func(t *testing.T) {
		t.Parallel()
		c := &fakeGroupRoutableCoordinator{groupByKey: map[string]uint64{
			"a": 0, "b": 0, "c": 1,
		}}
		got := LeaseReadGroupKeys(c, keys("a", "b", "c"))
		// gid 0 means unroutable: each is kept so the lease check still
		// runs and fails closed; the routable c collapses normally.
		equalKeySlices(t, keys("a", "b", "c"), got)
	})

	t.Run("empty input returns empty", func(t *testing.T) {
		t.Parallel()
		c := &fakeGroupRoutableCoordinator{groupByKey: map[string]uint64{}}
		require.Empty(t, LeaseReadGroupKeys(c, nil))
	})

	t.Run("non-routable coordinator returns input unchanged", func(t *testing.T) {
		t.Parallel()
		in := keys("a", "b", "c")
		got := LeaseReadGroupKeys(plainNonRoutable{}, in)
		equalKeySlices(t, in, got)
	})
}

// singleGroupLeaseCoordinator implements Coordinator + LeaseReadableCoordinator
// but NOT AllGroupsLeaseReadableCoordinator, modeling a single-group
// deployment. It records how many keyless LeaseRead calls it received so the
// fallback path can be asserted to issue exactly one.
type singleGroupLeaseCoordinator struct {
	Coordinator
	leaseReads int
	leaseErr   error
}

func (c *singleGroupLeaseCoordinator) LeaseRead(context.Context) (uint64, error) {
	c.leaseReads++
	return 0, c.leaseErr
}

func (c *singleGroupLeaseCoordinator) LeaseReadForKey(context.Context, []byte) (uint64, error) {
	panic("unused")
}

// allGroupsLeaseCoordinator implements AllGroupsLeaseReadableCoordinator and
// records how many groups a single LeaseReadAllGroups call fenced.
type allGroupsLeaseCoordinator struct {
	Coordinator
	allGroupsCalls int
	allGroupsErr   error
}

func (c *allGroupsLeaseCoordinator) LeaseReadAllGroups(context.Context) error {
	c.allGroupsCalls++
	return c.allGroupsErr
}

func TestLeaseReadAllGroupsThrough(t *testing.T) {
	t.Parallel()

	t.Run("single-group coordinator issues exactly one LeaseRead", func(t *testing.T) {
		t.Parallel()
		c := &singleGroupLeaseCoordinator{}
		require.NoError(t, LeaseReadAllGroupsThrough(c, context.Background()))
		require.Equal(t, 1, c.leaseReads,
			"single-group fallback must issue exactly one keyless lease read")
	})

	t.Run("single-group LeaseRead error propagates", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("boom")
		c := &singleGroupLeaseCoordinator{leaseErr: sentinel}
		err := LeaseReadAllGroupsThrough(c, context.Background())
		require.ErrorIs(t, err, sentinel)
	})

	t.Run("all-groups coordinator fences every group in one call", func(t *testing.T) {
		t.Parallel()
		c := &allGroupsLeaseCoordinator{}
		require.NoError(t, LeaseReadAllGroupsThrough(c, context.Background()))
		require.Equal(t, 1, c.allGroupsCalls)
	})

	t.Run("all-groups error propagates", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("group fence failed")
		c := &allGroupsLeaseCoordinator{allGroupsErr: sentinel}
		err := LeaseReadAllGroupsThrough(c, context.Background())
		require.ErrorIs(t, err, sentinel)
	})
}
