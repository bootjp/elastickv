package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type receiveRotationErrorCoordinator struct {
	stubAdapterCoordinator
	err error
}

func (c *receiveRotationErrorCoordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return nil, c.err
}

func TestSQSReceiveRotationClassifiesDispatchErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		wantSkip bool
		wantErr  error
	}{
		{name: "write conflict", err: store.ErrWriteConflict, wantSkip: true},
		{name: "txn locked", err: kv.ErrTxnLocked, wantSkip: true},
		{name: "route write fenced", err: kv.ErrRouteWriteFenced, wantErr: kv.ErrRouteWriteFenced},
	}

	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			const queueName = "receive-route-fence"
			const messageID = "msg-1"
			const gen = uint64(1)
			const visibleAt = int64(1000)

			meta := &sqsQueueMeta{Name: queueName, Generation: gen, PartitionCount: 1}
			rec := &sqsMessageRecord{
				MessageID:           messageID,
				Body:                []byte("body"),
				MD5OfBody:           sqsMD5Hex([]byte("body")),
				SendTimestampMillis: visibleAt,
				VisibleAtMillis:     visibleAt,
				QueueGeneration:     gen,
			}
			cand := sqsMsgCandidate{
				visKey:    sqsMsgVisKeyDispatch(meta, queueName, 0, gen, visibleAt, messageID),
				messageID: messageID,
				partition: 0,
			}
			dataKey := sqsMsgDataKeyDispatch(meta, queueName, 0, gen, messageID)
			srv := &SQSServer{
				coordinator: &receiveRotationErrorCoordinator{err: tt.err},
			}

			msg, skip, err := srv.commitReceiveRotation(
				context.Background(),
				queueName,
				meta,
				cand,
				dataKey,
				rec,
				uint64(visibleAt),
				sqsReceiveOptions{VisibilityTimeout: 30},
				nil,
				fifoLockAcquire,
			)

			require.Nil(t, msg)
			require.Equal(t, tt.wantSkip, skip)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
