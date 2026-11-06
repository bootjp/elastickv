package adapter

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/kv/kvtest"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

const fifoDelayTestQueue = "delay.fifo"

func newDelayTestServer(t *testing.T, st store.MVCCStore) *SQSServer {
	t.Helper()
	coord := kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{
		OCC: kvtest.OCCTransactions, ValidateReadKeys: true, AdvanceCommitTS: true,
	})
	coord.Clock().Observe(st.LastCommitTS())
	srv := NewSQSServer(nil, st, &fifoDelayLocalMembership{Coordinator: coord})
	t.Cleanup(srv.Stop)
	return srv
}

// LocalCoordinator has no Raft configuration. These tests explicitly model
// one local member; unknown production membership is never treated as local.
type fifoDelayLocalMembership struct{ kv.Coordinator }

func (*fifoDelayLocalMembership) RaftMembers(context.Context) ([]kv.RaftMember, error) {
	return []kv.RaftMember{{NodeID: "local", Address: "local", Suffrage: "voter"}}, nil
}

func (c *fifoDelayLocalMembership) RaftMembersForKey(ctx context.Context, _ []byte) ([]kv.RaftMember, error) {
	return c.RaftMembers(ctx)
}

func createDelayTestQueue(t *testing.T, srv *SQSServer, partitions uint32, delay int64) {
	t.Helper()
	attrs := map[string]string{"FifoQueue": "true", "DelaySeconds": strconv.FormatInt(delay, 10)}
	if partitions > 1 {
		attrs["PartitionCount"], attrs["FifoThroughputLimit"], attrs["DeduplicationScope"] = fmt.Sprint(partitions), "perMessageGroupId", "messageGroup"
	}
	_, err := srv.createQueueCore(context.Background(), &sqsCreateQueueInput{QueueName: fifoDelayTestQueue, Attributes: attrs})
	require.NoError(t, err)
}

func sendDelayTestMessage(t *testing.T, srv *SQSServer, body, group string) string {
	t.Helper()
	out, err := srv.sendMessageCore(context.Background(), fifoDelayTestQueue, sqsSendMessageInput{MessageBody: body, MessageGroupId: group, MessageDeduplicationId: body})
	require.NoError(t, err)
	return out["MessageId"]
}

func beginDelayTestUpdate(t *testing.T, srv *SQSServer, delay int64) {
	t.Helper()
	_, _, _, _, done, err := srv.trySetQueueAttributesOnce(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": strconv.FormatInt(delay, 10)})
	require.NoError(t, err)
	require.True(t, done)
	meta, exists, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
	require.NoError(t, err)
	require.True(t, exists)
	require.NotNil(t, meta.DelayReindex)
}

func requireDelayUnavailable(t *testing.T, err error) {
	t.Helper()
	var api *sqsAPIError
	require.ErrorAs(t, err, &api)
	require.Equal(t, http.StatusServiceUnavailable, api.status)
	require.Equal(t, sqsErrServiceUnavailable, api.errorType)
}

func TestSQSFIFODelayRecoveryFromPebble(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		for _, initial := range []int64{0, 900} {
			t.Run(fmt.Sprintf("partitions_%d/from_%d", partitions, initial), func(t *testing.T) {
				dir := t.TempDir()
				st, err := store.NewPebbleStore(dir)
				require.NoError(t, err)
				srv := newDelayTestServer(t, st)
				createDelayTestQueue(t, srv, partitions, initial)
				ids := make([]string, sqsFifoDelayPageSize*2+3)
				for i := range ids {
					ids[i] = sendDelayTestMessage(t, srv, fmt.Sprint(i), "g")
				}
				target := int64(900) - initial
				beginDelayTestUpdate(t, srv, target)
				// An empty partition is still a durable unit of progress in HT mode.
				done, err := srv.advanceFifoDelayReindex(context.Background(), fifoDelayTestQueue)
				require.NoError(t, err)
				require.False(t, done)
				before, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
				require.NoError(t, err)
				require.NotNil(t, before.DelayReindex)
				require.NoError(t, st.Close())
				// Reopen the actual on-disk database with a new coordinator/server;
				// no process-local job state survives this boundary.
				st, err = store.NewPebbleStore(dir)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, st.Close()) })
				srv = newDelayTestServer(t, st)
				after, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
				require.NoError(t, err)
				require.Equal(t, before.DelayReindex, after.DelayReindex)
				_, err = srv.sendMessageCore(context.Background(), fifoDelayTestQueue, sqsSendMessageInput{MessageBody: "blocked", MessageGroupId: "g", MessageDeduplicationId: "blocked"})
				requireDelayUnavailable(t, err)
				_, err = srv.scanAndDeliverOnce(context.Background(), fifoDelayTestQueue, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
				requireDelayUnavailable(t, err)
				for i := 0; i < len(ids)+int(partitions); i++ {
					require.NoError(t, srv.reapAllQueues(context.Background()))
					meta, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
					require.NoError(t, err)
					if meta.DelayReindex == nil {
						break
					}
				}
				meta, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
				require.NoError(t, err)
				require.Nil(t, meta.DelayReindex, "background recovery must finish without another configuration request")
				for _, id := range ids {
					rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
					require.Equal(t, rec.SendTimestampMillis+target*1000, rec.VisibleAtMillis)
					require.Equal(t, rec.VisibleAtMillis, rec.AvailableAtMillis)
					old := sqsMsgVisKeyDispatch(meta, meta.Name, partitionFor(meta, "g"), meta.Generation, rec.SendTimestampMillis+initial*1000, id)
					_, err := st.GetAt(context.Background(), old, st.LastCommitTS())
					require.ErrorIs(t, err, store.ErrKeyNotFound)
					newKey := sqsMsgVisKeyDispatch(meta, meta.Name, partitionFor(meta, "g"), meta.Generation, rec.VisibleAtMillis, id)
					value, err := st.GetAt(context.Background(), newKey, st.LastCommitTS())
					require.NoError(t, err)
					require.Equal(t, id, string(value))
				}
			})
		}
	}
}

type fifoDelayBarrierCoordinator struct {
	kv.Coordinator
	match   func(*kv.OperationGroup[kv.OP]) bool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (c *fifoDelayBarrierCoordinator) RaftMembers(ctx context.Context) ([]kv.RaftMember, error) {
	provider, ok := c.Coordinator.(kv.RaftMembershipCoordinator)
	if !ok {
		return nil, kv.ErrLeaderNotFound
	}
	return provider.RaftMembers(ctx)
}

func (c *fifoDelayBarrierCoordinator) RaftMembersForKey(ctx context.Context, key []byte) ([]kv.RaftMember, error) {
	provider, ok := c.Coordinator.(kv.RaftMembershipCoordinator)
	if !ok {
		return nil, kv.ErrLeaderNotFound
	}
	return provider.RaftMembersForKey(ctx, key)
}

func (c *fifoDelayBarrierCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if c.match(req) {
		take := false
		c.once.Do(func() { take = true })
		if take {
			close(c.entered)
			select {
			case <-c.release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return c.Coordinator.Dispatch(ctx, req)
}

func delayDataPut(req *kv.OperationGroup[kv.OP]) bool {
	for _, op := range req.Elems {
		if op.Op == kv.Put && (bytes.HasPrefix(op.Key, []byte(SqsMsgDataPrefix)) || bytes.HasPrefix(op.Key, []byte(SqsPartitionedMsgDataPrefix))) {
			return true
		}
	}
	return false
}

func TestSQSFIFODelayFencesStaleReceive(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		t.Run(fmt.Sprint(partitions), func(t *testing.T) {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			srv := newDelayTestServer(t, st)
			createDelayTestQueue(t, srv, partitions, 0)
			id := sendDelayTestMessage(t, srv, "head", "g")
			gate := &fifoDelayBarrierCoordinator{Coordinator: srv.coordinator, match: delayDataPut, entered: make(chan struct{}), release: make(chan struct{})}
			srv.coordinator = gate
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				messages, err := srv.scanAndDeliverOnce(ctx, fifoDelayTestQueue, sqsReceiveOptions{Max: 1, VisibilityTimeout: 60})
				if len(messages) > 0 {
					err = errors.New("stale receive crossed delay fence")
				}
				result <- err
			}()
			var unblock sync.Once
			t.Cleanup(func() { unblock.Do(func() { close(gate.release) }); cancel() })
			select {
			case <-gate.entered:
			case <-ctx.Done():
				t.Fatal("receive never reached its commit")
			}
			beginDelayTestUpdate(t, srv, 900)
			unblock.Do(func() { close(gate.release) })
			require.NoError(t, <-result)
			require.NoError(t, srv.completeFifoDelayReindex(ctx, fifoDelayTestQueue))
			rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
			require.Zero(t, rec.ReceiveCount)
			require.Equal(t, rec.SendTimestampMillis+900_000, rec.VisibleAtMillis)
		})
	}
}

func TestSQSFIFODelayFencesStaleSend(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 4, 900)
	first := sendDelayTestMessage(t, srv, "A", "g")
	gate := &fifoDelayBarrierCoordinator{Coordinator: srv.coordinator, match: delayDataPut, entered: make(chan struct{}), release: make(chan struct{})}
	srv.coordinator = gate
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type result struct {
		out map[string]string
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := srv.sendMessageCore(ctx, fifoDelayTestQueue, sqsSendMessageInput{MessageBody: "B", MessageGroupId: "g", MessageDeduplicationId: "B"})
		done <- result{out, err}
	}()
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(gate.release) }); cancel() })
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("send never reached its commit")
	}
	require.NoError(t, srv.setQueueAttributesCore(ctx, fifoDelayTestQueue, map[string]string{"DelaySeconds": "0"}))
	unblock.Do(func() { close(gate.release) })
	got := <-done
	require.NoError(t, got.err)
	second := loadDelayTestRecord(t, srv, fifoDelayTestQueue, got.out["MessageId"], "g")
	require.Equal(t, second.SendTimestampMillis, second.VisibleAtMillis, "retry must rederive delay under a fresh snapshot")
	messages, err := srv.scanAndDeliverOnce(ctx, fifoDelayTestQueue, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, first, messages[0]["MessageId"])
}

func TestSQSFIFODelayPageByteBudget(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 1, 0)
	ids := make([]string, 8)
	for i := range ids {
		ids[i] = sendDelayTestMessage(t, srv, strings.Repeat("x", 200_000)+fmt.Sprint(i), "g")
	}
	beginDelayTestUpdate(t, srv, 900)
	done, err := srv.advanceFifoDelayReindex(context.Background(), fifoDelayTestQueue)
	require.NoError(t, err)
	require.False(t, done, "byte budget must stop before this short scan page is exhausted")
	changed := 0
	for _, id := range ids {
		rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
		if rec.VisibleAtMillis != rec.SendTimestampMillis {
			changed++
		}
	}
	require.Positive(t, changed)
	require.Less(t, changed, len(ids))
	require.NoError(t, srv.completeFifoDelayReindex(context.Background(), fifoDelayTestQueue))
	for _, id := range ids {
		rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
		require.Equal(t, rec.SendTimestampMillis+900_000, rec.VisibleAtMillis)
	}
}

func TestSQSFIFODelayPageAllowsOneAcceptedRecordOvershoot(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 1, 0)
	// The body is at the adapter's allowed maximum. The attribute and body
	// fit its public request limit, but base64 storage encoding expands the
	// accepted record beyond the per-page mutation-byte budget.
	const requestHeadroom = 8 << 10
	in := sqsSendMessageInput{
		QueueUrl:       "http://localhost/queue/" + fifoDelayTestQueue,
		MessageBody:    strings.Repeat("x", sqsMaximumAllowedMaximumMessageSize),
		MessageGroupId: "g",
		MessageAttributes: map[string]sqsMessageAttributeValue{
			"large": {DataType: "String", StringValue: strings.Repeat("y", sqsMaxRequestBodyBytes-sqsMaximumAllowedMaximumMessageSize-requestHeadroom)},
		},
	}
	ids := make([]string, 2)
	for i := range ids {
		in.MessageDeduplicationId = fmt.Sprint(i)
		wire, err := json.Marshal(in)
		require.NoError(t, err)
		require.Less(t, len(wire), sqsMaxRequestBodyBytes)
		out, err := srv.sendMessageCore(context.Background(), fifoDelayTestQueue, in)
		require.NoError(t, err, "an accepted record must not later stall reindex forever")
		ids[i] = out["MessageId"]
	}
	beginDelayTestUpdate(t, srv, 900)
	meta, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
	require.NoError(t, err)
	firstCost := make(map[string]int, len(ids))
	for _, id := range ids {
		rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
		_, cost, err := fifoDelayRecordOps(meta, 0, rec, sqsMsgDataKeyDispatch(meta, meta.Name, 0, meta.Generation, id))
		require.NoError(t, err)
		require.Greater(t, cost, sqsFifoDelayByteLimit)
		firstCost[id] = cost
	}
	recording := &fifoAttemptCoordinator{Coordinator: srv.coordinator}
	srv.coordinator = recording
	done, err := srv.advanceFifoDelayReindex(context.Background(), fifoDelayTestQueue)
	require.NoError(t, err)
	require.False(t, done)
	require.Len(t, recording.requests, 1)
	pageCost, rewritten := 0, 0
	var rewrittenID string
	for _, op := range recording.requests[0].Elems {
		if bytes.Equal(op.Key, sqsQueueMetaKey(meta.Name)) {
			continue
		}
		pageCost += len(op.Key) + len(op.Value)
		if op.Op == kv.Put && bytes.HasPrefix(op.Key, []byte(SqsMsgDataPrefix)) {
			rec, err := decodeSQSMessageRecord(op.Value)
			require.NoError(t, err)
			rewritten++
			rewrittenID = rec.MessageID
		}
	}
	require.Equal(t, 1, rewritten, "an oversized first record must exclude every following record from that page")
	require.Equal(t, firstCost[rewrittenID], pageCost, "the overshoot must be exactly one finite accepted record's mutation cost")
	require.NoError(t, srv.completeFifoDelayReindex(context.Background(), fifoDelayTestQueue))
	for _, id := range ids {
		rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
		require.Equal(t, rec.SendTimestampMillis+900_000, rec.VisibleAtMillis)
	}
}

func TestSQSFIFODelayPurgeCancelsJob(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 4, 900)
	sendDelayTestMessage(t, srv, "A", "g")
	beginDelayTestUpdate(t, srv, 0)
	require.NoError(t, srv.purgeQueueCore(context.Background(), fifoDelayTestQueue))
	require.NoError(t, srv.completeFifoDelayReindex(context.Background(), fifoDelayTestQueue))
	id := sendDelayTestMessage(t, srv, "new-generation", "g")
	messages, err := srv.scanAndDeliverOnce(context.Background(), fifoDelayTestQueue, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, id, messages[0]["MessageId"])
}
