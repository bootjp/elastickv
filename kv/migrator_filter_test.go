package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A transaction-family bracket holds the user key wrapped in !txn|int| and
// friends. The group filter must resolve those through the same partition
// resolver that decides the embedded row's own bracket: a partitioned HT-FIFO
// SQS row and its intent belong to one group, and answering differently for
// the two is how a migration copies the data and strands the transaction
// state.
func TestRouteKeyFilterForGroupResolvesTxnWrappedPartitionedKeys(t *testing.T) {
	t.Parallel()

	const (
		sourceGroup = uint64(7)
		otherGroup  = uint64(9)
	)
	partitioned := []byte("!sqs|msg|data|p|q|0|m1")
	elsewhere := []byte("!sqs|msg|data|p|q|1|m1")
	resolver := &fakePartitionResolver{
		routes: map[string]uint64{
			string(partitioned): sourceGroup,
			string(elsewhere):   otherGroup,
		},
		recognisedPrefix: []byte("!sqs|msg|data|p|"),
	}

	// A route range that excludes the !sqs|route|global collapse target, so a
	// filter that falls through to the byte-range path answers false.
	filter := RouteKeyFilterForGroup([]byte("a"), []byte("b"), sourceGroup, resolver)

	for _, tc := range []struct {
		name string
		key  []byte
		want bool
	}{
		{name: "bare partitioned row", key: partitioned, want: true},
		{name: "intent", key: txnIntentKey(partitioned), want: true},
		{name: "lock", key: txnLockKey(partitioned), want: true},
		{name: "meta", key: append(append([]byte{}, txnMetaPrefixBytes...), partitioned...), want: true},
		{name: "intent for another group", key: txnIntentKey(elsewhere), want: false},
		{name: "bare row for another group", key: elsewhere, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, filter(tc.key), "key %q", tc.key)
		})
	}
}

// A recognised-but-unresolved partitioned key fails closed whether or not it
// arrives wrapped: the resolver cannot say which group owns it, and guessing
// through the byte-range route would export it from the wrong source.
func TestRouteKeyFilterForGroupFailsClosedOnWrappedUnresolvedKey(t *testing.T) {
	t.Parallel()

	unresolved := []byte("!sqs|msg|data|p|unknown|0|m1")
	resolver := &fakePartitionResolver{
		routes:           map[string]uint64{},
		recognisedPrefix: []byte("!sqs|msg|data|p|"),
	}
	filter := RouteKeyFilterForGroup(nil, nil, 7, resolver)

	require.False(t, filter(unresolved))
	require.False(t, filter(txnIntentKey(unresolved)))
	require.False(t, filter(txnCommitKey(unresolved, 42)))
}
