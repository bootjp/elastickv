package kv

import (
	"encoding/base64"
	"encoding/binary"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

// partitionedSQSMsgDataKey builds an HT-FIFO partitioned message row the way
// the SQS adapter lays one out: family prefix, partition discriminator,
// base64url queue segment, terminator, then the big-endian partition number.
func partitionedSQSMsgDataKey(queue string, partition uint32) []byte {
	key := []byte(sqsMsgDataPrefix + sqsPartitionMarker)
	key = append(key, base64.RawURLEncoding.AppendEncode(nil, []byte(queue))...)
	key = append(key, '|')
	return binary.BigEndian.AppendUint32(key, partition)
}

// Cleanup runs inside FSM apply, where the partition resolver is unavailable:
// it is process-local config (--sqsFifoPartitionMap), so consulting it would
// make apply diverge across replicas. routeKey collapses every partitioned SQS
// row to the one global SQS route key, so a migrated interval covering that key
// would otherwise look like it owns every partition -- including the ones the
// resolver still routes to this group, whose rows the export (which does have
// the resolver) never claimed. Deleting those loses messages still served from
// here.
func TestMigrationCleanupSkipsResolverOwnedSQSRows(t *testing.T) {
	t.Parallel()

	// A range that covers the global SQS route key, which is what every
	// partitioned row collapses to.
	opts := migrationCleanupOptionsFromProto(&pb.CleanupMigrationRequest{
		JobId:                 7,
		RangeStart:            []byte("!sqs|"),
		RangeEnd:              []byte("!sqs~"),
		RouteStart:            []byte("!"),
		RouteEnd:              nil,
		RequiresRouteKeyCheck: true,
	}, 1)
	require.NotNil(t, opts.AcceptVersion)

	partitioned := partitionedSQSMsgDataKey("q", 3)
	require.Equal(t, sqsGlobalRouteKey, routeKey(partitioned),
		"the route key really does collapse to the global SQS key")
	require.False(t, opts.AcceptVersion(partitioned, []byte("body")),
		"a resolver-owned partitioned row must not be deleted by catalog cleanup")

	// A non-partitioned SQS row in the same interval is catalog-routed and
	// still gets cleaned up, so the guard does not disable cleanup wholesale.
	legacy := append([]byte(sqsMsgDataPrefix), []byte("q|1")...)
	require.True(t, opts.AcceptVersion(legacy, []byte("body")))
}

func TestIsPartitionedSQSKey(t *testing.T) {
	t.Parallel()

	for _, family := range []string{
		sqsMsgDataPrefix, sqsMsgVisPrefix, sqsMsgDedupPrefix,
		sqsMsgGroupPrefix, sqsMsgByAgePrefix,
	} {
		require.True(t, IsPartitionedSQSKey([]byte(family+sqsPartitionMarker+"q")), family)
		require.False(t, IsPartitionedSQSKey([]byte(family+"q")), family)
	}
	// Queue metadata is not partitioned.
	require.False(t, IsPartitionedSQSKey([]byte(sqsQueueMetaPrefix+sqsPartitionMarker+"q")))
	require.False(t, IsPartitionedSQSKey(nil))
	require.False(t, IsPartitionedSQSKey([]byte("!redis|k")))
}
