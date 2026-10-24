package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

// Migration cleanup runs inside FSM apply and cannot consult the partition
// resolver, so it recognises resolver-owned rows structurally via
// kv.IsPartitionedSQSKey. That predicate carries its own copy of the family
// prefixes, so it has to be pinned against the keys the adapter actually
// writes: a family renamed or added here without updating kv would leave those
// rows exposed to deletion by a catalog route cleanup that never owned them.
func TestPartitionedSQSPrefixesAlign(t *testing.T) {
	t.Parallel()

	const (
		queue     = "q"
		partition = uint32(3)
		gen       = uint64(1)
	)
	for name, key := range map[string][]byte{
		"data":  sqsPartitionedMsgDataKey(queue, partition, gen, "m1"),
		"vis":   sqsPartitionedMsgVisKey(queue, partition, gen, 100, "m1"),
		"dedup": sqsPartitionedMsgDedupKey(queue, partition, gen, "g1", "d1"),
		"group": sqsPartitionedMsgGroupKey(queue, partition, gen, "g1"),
		"byage": sqsPartitionedMsgByAgeKey(queue, partition, gen, 100, "m1"),
	} {
		require.True(t, kv.IsPartitionedSQSKey(key), "%s must be recognised as partitioned", name)
	}

	// The legacy, catalog-routed layouts must stay outside the guard, or
	// cleanup would stop reclaiming the rows it does own.
	for name, key := range map[string][]byte{
		"legacy data": sqsMsgDataKey(queue, gen, "m1"),
		"queue meta":  sqsQueueMetaKey(queue),
	} {
		require.False(t, kv.IsPartitionedSQSKey(key), "%s must not be recognised as partitioned", name)
	}
}
