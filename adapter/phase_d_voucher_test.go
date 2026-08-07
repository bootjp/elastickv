package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func newPhaseDVoucherCoordinator(st store.MVCCStore) *distributionCoordinatorStub {
	coord := newDistributionCoordinatorStub(st, true)
	coord.allocator = &distributionTSOAllocator{base: 100, phaseD: true, phaseDFloor: 10}
	return coord
}

func TestSQSPhaseDQueueAndSendPathsBindReadVoucher(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newPhaseDVoucherCoordinator(st)
	server := NewSQSServer(nil, st, coord)

	_, err := server.createQueueCore(ctx, &sqsCreateQueueInput{QueueName: "phase-d-voucher"})
	require.NoError(t, err)
	require.Equal(t, 1, coord.vouchCalls)

	_, err = server.sendMessageCore(ctx, "phase-d-voucher", sqsSendMessageInput{MessageBody: "one"})
	require.NoError(t, err)
	require.Equal(t, 2, coord.vouchCalls)

	entries := []sqsSendMessageBatchEntryInput{{Id: "a", MessageBody: "two"}}
	identities := make([]sqsSendIdentity, len(entries))
	successful, failed, retry, err := server.trySendMessageBatchOnce(ctx, "phase-d-voucher", entries, identities)
	require.NoError(t, err)
	require.False(t, retry)
	require.Empty(t, failed)
	require.Len(t, successful, 1)
	require.Equal(t, 3, coord.vouchCalls)

	err = server.purgeQueueCore(ctx, "phase-d-voucher")
	require.NoError(t, err)
	require.Equal(t, 4, coord.vouchCalls)

	err = server.tagQueueCore(ctx, "phase-d-voucher", map[string]string{"env": "test"})
	require.NoError(t, err)
	require.Equal(t, 5, coord.vouchCalls)

	err = server.setQueueAttributesCore(ctx, "phase-d-voucher", map[string]string{"VisibilityTimeout": "45"})
	require.NoError(t, err)
	require.Equal(t, 6, coord.vouchCalls)

	err = server.deleteQueueCore(ctx, "phase-d-voucher")
	require.NoError(t, err)
	require.Equal(t, 7, coord.vouchCalls)
	require.LessOrEqual(t, coord.lastStartTS, uint64(10))
}

func TestDynamoDBCreateTablePhaseDBindsReadVoucher(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newPhaseDVoucherCoordinator(st)
	server := NewDynamoDBServer(nil, st, coord)

	err := server.createTableWithRetry(ctx, "phase-d-voucher", &dynamoTableSchema{
		TableName:            "phase-d-voucher",
		AttributeDefinitions: map[string]string{"pk": "S"},
		PrimaryKey:           dynamoKeySchema{HashKey: "pk"},
		KeyEncodingVersion:   dynamoOrderedKeyEncodingV2,
	})
	require.NoError(t, err)
	require.Equal(t, 1, coord.vouchCalls)
	require.Equal(t, uint64(1), coord.lastStartTS)
}

var _ kv.Coordinator = (*distributionCoordinatorStub)(nil)
