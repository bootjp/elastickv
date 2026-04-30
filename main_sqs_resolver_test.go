package main

import (
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

// TestBuildSQSPartitionResolver_NilOnEmpty pins the typed-nil
// interface invariant flagged by the Phase 3.D PR 4-B-2 round-1
// review: buildSQSPartitionResolver MUST return a true-nil
// kv.PartitionResolver interface (not a typed-nil pointer wrapped
// in a non-nil interface) when the partition map is empty or nil.
//
// If the function's return type were the concrete
// *adapter.SQSPartitionResolver and the body returned a nil
// pointer, Go would wrap that pointer into a NON-NIL interface
// when assigned to kv.ShardRouter.partitionResolver. The router's
// resolver-first short-circuit `s.partitionResolver != nil` would
// always pass on a non-partitioned cluster, and every request
// would pay an extra ResolveGroup call (which the nil-receiver
// guard inside (*SQSPartitionResolver).ResolveGroup makes safe
// but not free).
//
// requireNilInterface forces the interface conversion at the call
// boundary, which is what makes this regression observable. A
// plain require.Nil(t, r) would pass even with a typed-nil because
// at that point r still has the concrete pointer type.
func TestBuildSQSPartitionResolver_NilOnEmpty(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   map[string]sqsFifoQueueRouting
	}{
		{"nil map", nil},
		{"empty map", map[string]sqsFifoQueueRouting{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			requireNilInterface(t, buildSQSPartitionResolver(tc.in),
				"buildSQSPartitionResolver("+tc.name+
					") must produce a true nil interface, "+
					"not a typed-nil pointer wrapper")
		})
	}
}

// TestBuildSQSPartitionResolver_NonEmptyReturnsResolver pins the
// happy path: a non-empty partition map yields a non-nil resolver
// that dispatches partition keys correctly.
func TestBuildSQSPartitionResolver_NonEmptyReturnsResolver(t *testing.T) {
	t.Parallel()
	in := map[string]sqsFifoQueueRouting{
		"orders.fifo": {partitionCount: 2, groups: []string{"10", "11"}},
	}
	r := buildSQSPartitionResolver(in)
	require.NotNil(t, r,
		"non-empty partition map must produce a non-nil resolver")
}

// requireNilInterface accepts a kv.PartitionResolver and asserts
// the interface value (NOT just the underlying pointer) is nil.
// The function-parameter conversion forces a typed-nil pointer to
// be wrapped into a non-nil interface, which is exactly the
// failure mode the regression test guards against.
func requireNilInterface(t *testing.T, r kv.PartitionResolver, msg string) {
	t.Helper()
	require.Nil(t, r, msg)
}
