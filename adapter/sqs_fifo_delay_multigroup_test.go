package adapter

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraft "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// Only the reply is injected: every recorded PREPARE/COMMIT first reaches the
// real Raft-backed transaction implementation and its KV state machine.
type sqsDelayRaftInterruption struct {
	mu       sync.Mutex
	armed    bool
	dropped  bool
	prepares map[uint64]*pb.Request
	primary  *pb.Request
	index    uint64
}

type sqsDelayRaftTransaction struct {
	kv.Transactional
	group uint64
	fault *sqsDelayRaftInterruption
}

func (x *sqsDelayRaftTransaction) Commit(ctx context.Context, reqs []*pb.Request) (*kv.TransactionResponse, error) {
	resp, err := x.Transactional.Commit(ctx, reqs)
	if err != nil || len(reqs) != 1 {
		return resp, err
	}
	x.fault.mu.Lock()
	defer x.fault.mu.Unlock()
	if !x.fault.armed || x.fault.dropped {
		return resp, nil
	}
	switch reqs[0].Phase {
	case pb.Phase_PREPARE:
		x.fault.prepares[x.group] = proto.CloneOf(reqs[0])
	case pb.Phase_COMMIT:
		if x.group == 2 {
			x.fault.dropPrimary(reqs[0], resp)
			return nil, errors.New("injected lost reply after real primary COMMIT")
		}
	case pb.Phase_NONE, pb.Phase_ABORT:
		return resp, nil
	default:
		return resp, nil
	}
	return resp, nil
}

// Called under mu, after the underlying Raft Commit actually returned.
func (f *sqsDelayRaftInterruption) dropPrimary(req *pb.Request, resp *kv.TransactionResponse) {
	f.dropped = true
	f.primary = proto.CloneOf(req)
	if resp != nil {
		f.index = resp.CommitIndex
	}
}

// Keep the underlying leader proxy's connection lifecycle intact.
func (x *sqsDelayRaftTransaction) Close() error {
	if closer, ok := x.Transactional.(interface{ Close() error }); ok {
		return closer.Close()
	}
	return nil
}

type sqsDelayPageObserver struct {
	kv.CoordinatorForwarder
	mu        sync.Mutex
	pageCalls int
	pageError error
}

func (o *sqsDelayPageObserver) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	resp, err := o.CoordinatorForwarder.Dispatch(ctx, req)
	if delayDataPut(req) {
		for _, elem := range req.Elems {
			if elem.Op == kv.Put && bytes.Equal(elem.Key, sqsQueueMetaKey(fifoDelayTestQueue)) {
				o.mu.Lock()
				o.pageCalls++
				o.pageError = err
				o.mu.Unlock()
				break
			}
		}
	}
	return resp, err
}

type sqsDelayMultiGroup struct {
	server *SQSServer
	http   *httptest.Server
	groups map[uint64]*kv.ShardGroup
	shards *kv.ShardStore
	coord  *kv.ShardedCoordinator
	trace  *sqsDelayPageObserver
	fault  *sqsDelayRaftInterruption
	close  func()
}

func openSQSDelayMultiGroup(t *testing.T, dir string, bootstrap bool) *sqsDelayMultiGroup {
	t.Helper()
	f := &sqsDelayMultiGroup{groups: make(map[uint64]*kv.ShardGroup),
		fault: &sqsDelayRaftInterruption{prepares: make(map[uint64]*pb.Request)}}
	var once sync.Once
	f.close = func() {
		once.Do(func() {
			if f.http != nil {
				f.http.Close()
			}
			if f.server != nil {
				f.server.Stop()
			}
			if f.coord != nil {
				require.NoError(t, f.coord.Close())
			}
			if f.shards != nil {
				require.NoError(t, f.shards.Close())
			}
			for _, group := range f.groups {
				if group.Engine != nil {
					require.NoError(t, group.Engine.Close())
				}
				require.NoError(t, group.Store.Close())
			}
		})
	}
	t.Cleanup(f.close)
	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, nil, 1)
	for _, gid := range []uint64{1, 2} {
		st, err := store.NewPebbleStore(filepath.Join(dir, fmt.Sprintf("store-%d", gid)))
		require.NoError(t, err)
		group := &kv.ShardGroup{Store: st}
		f.groups[gid] = group
		id := fmt.Sprintf("sqs-delay-g%d", gid)
		engine, err := etcdraft.Open(context.Background(), etcdraft.OpenConfig{
			LocalID: id, LocalAddress: id, Bootstrap: bootstrap,
			DataDir:      filepath.Join(dir, fmt.Sprintf("raft-%d", gid)),
			StateMachine: kv.NewKvFSMWithHLC(st, kv.NewHLC()),
			TickInterval: 10 * time.Millisecond, HeartbeatTick: 1, ElectionTick: 10,
			MaxSizePerMsg: 1 << 20, MaxInflightMsg: 256,
		})
		require.NoError(t, err)
		group.Engine = engine
		require.Eventually(t, func() bool { return engine.State() == raftengine.StateLeader },
			5*time.Second, 10*time.Millisecond)
		group.Txn = kv.NewLeaderProxyWithEngine(engine)
	}
	resolver := NewSQSPartitionResolver(map[string][]uint64{fifoDelayTestQueue: {2, 2, 2, 2}})
	f.shards = kv.NewShardStore(routes, f.groups).WithPartitionResolver(resolver)
	f.coord = kv.NewShardedCoordinator(routes, f.groups, 1, kv.NewHLC(), f.shards).
		WithAllShardGroups(1, 2).WithPartitionResolver(resolver)
	for gid, group := range f.groups {
		group.Txn = &sqsDelayRaftTransaction{Transactional: group.Txn, group: gid, fault: f.fault}
	}
	f.trace = &sqsDelayPageObserver{CoordinatorForwarder: kv.CoordinatorForwarder{Inner: f.coord}}
	f.server = NewSQSServer(nil, f.shards, f.trace, WithSQSPartitionResolver(resolver))
	f.http = httptest.NewServer(http.HandlerFunc(f.server.handle))
	f.server.leaderSQS = map[string]string{"sqs-delay-g1": f.http.URL, "sqs-delay-g2": f.http.URL}
	return f
}

func (f *sqsDelayMultiGroup) call(t *testing.T, ctx context.Context, target string, input any) (int, map[string]any) {
	t.Helper()
	body, err := json.Marshal(input)
	require.NoError(t, err)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.http.URL+"/", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", sqsContentTypeJSON)
	resp, err := f.http.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return resp.StatusCode, out
}

func (f *sqsDelayMultiGroup) rawMeta(t *testing.T, ctx context.Context) *sqsQueueMeta {
	t.Helper()
	raw, err := f.groups[1].Store.GetAt(ctx, sqsQueueMetaKey(fifoDelayTestQueue), ^uint64(0))
	require.NoError(t, err)
	meta, err := decodeSQSQueueMeta(raw)
	require.NoError(t, err)
	return meta
}

func (f *sqsDelayMultiGroup) seed(t *testing.T, ctx context.Context) (string, *sqsQueueMeta, []string) {
	t.Helper()
	status, out := f.call(t, ctx, sqsCreateQueueTarget, map[string]any{
		"QueueName": fifoDelayTestQueue, "Attributes": map[string]string{
			"FifoQueue": "true", "PartitionCount": "4", "DelaySeconds": "0",
			"FifoThroughputLimit": "perMessageGroupId", "DeduplicationScope": "messageGroup",
		},
	})
	require.Equal(t, http.StatusOK, status, out)
	url := delayNativeString(t, out, "QueueUrl")
	meta := f.rawMeta(t, ctx)
	group := delayGroupOnPartition(meta, 0)
	ids := make([]string, sqsFifoDelayPageSize+2)
	for i := range ids {
		status, out = f.call(t, ctx, sqsSendMessageTarget, map[string]any{
			"QueueUrl": url, "MessageBody": fmt.Sprintf("message-%02d", i),
			"MessageGroupId": group, "MessageDeduplicationId": fmt.Sprint(i),
		})
		require.Equal(t, http.StatusOK, status, out)
		ids[i] = delayNativeString(t, out, "MessageId")
	}
	status, out = f.call(t, ctx, sqsSendMessageTarget, map[string]any{
		"QueueUrl": url, "MessageBody": "message-00",
		"MessageGroupId": group, "MessageDeduplicationId": "0",
	})
	require.Equal(t, http.StatusOK, status, out)
	require.Equal(t, ids[0], delayNativeString(t, out, "MessageId"), "dedup lookup must use the actual partition owner rather than enqueue another message")
	metaGID, err := f.coord.ResolveOperationGroupForKey(sqsQueueMetaKey(meta.Name))
	require.NoError(t, err)
	require.EqualValues(t, 1, metaGID)
	dataGID, err := f.coord.ResolveOperationGroupForKey(sqsMsgDataKeyDispatch(meta, meta.Name, 0, meta.Generation, ids[0]))
	require.NoError(t, err)
	require.EqualValues(t, 2, dataGID)
	return url, meta, ids
}

func TestSQSFIFODelayMultiGroupUninterruptedPage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := openSQSDelayMultiGroup(t, t.TempDir(), true)
	url, _, ids := f.seed(t, ctx)
	status, out := f.call(t, ctx, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url, "Attributes": map[string]string{"DelaySeconds": "900"},
	})
	require.Equal(t, http.StatusOK, status, out)
	meta := f.rawMeta(t, ctx)
	require.Nil(t, meta.DelayReindex)
	require.True(t, meta.FifoOrderIndexed)
	f.trace.mu.Lock()
	pageCalls, pageError := f.trace.pageCalls, f.trace.pageError
	f.trace.mu.Unlock()
	require.NoError(t, pageError)
	require.Equal(t, 2, pageCalls, "the control must span two nonempty cross-group pages")
	for _, id := range ids {
		key := sqsMsgDataKeyDispatch(meta, meta.Name, 0, meta.Generation, id)
		raw, err := f.shards.GetGroupAt(ctx, 2, key, f.server.nextTxnReadTS(ctx))
		require.NoError(t, err)
		rec, err := decodeSQSMessageRecord(raw)
		require.NoError(t, err)
		require.Equal(t, rec.SendTimestampMillis+900_000, rec.VisibleAtMillis)
	}
}

func TestSQSFIFODelayMultiGroupPrimaryReplyLoss(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dir := t.TempDir()
	f := openSQSDelayMultiGroup(t, dir, true)
	url, meta, ids := f.seed(t, ctx)

	f.fault.mu.Lock()
	f.fault.armed = true
	f.fault.mu.Unlock()
	status, out := f.call(t, ctx, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url, "Attributes": map[string]string{"DelaySeconds": "900"},
	})
	require.Equal(t, http.StatusInternalServerError, status, out)
	f.trace.mu.Lock()
	pageCalls, pageError := f.trace.pageCalls, f.trace.pageError
	f.trace.mu.Unlock()
	require.ErrorIs(t, pageError, kv.ErrTransactionOutcomeUnknown)
	require.Equal(t, 1, pageCalls, "the adapter must not blindly retry an unknown page decision")

	f.fault.mu.Lock()
	dropped, primary, index := f.fault.dropped, f.fault.primary, f.fault.index
	metaPrepare, dataPrepare := f.fault.prepares[1], f.fault.prepares[2]
	f.fault.mu.Unlock()
	require.True(t, dropped)
	require.NotNil(t, primary)
	require.NotZero(t, index)
	require.GreaterOrEqual(t, f.groups[2].Engine.Status().AppliedIndex, index)
	require.NotNil(t, metaPrepare)
	require.NotNil(t, dataPrepare)
	require.Equal(t, primary.Ts, metaPrepare.Ts)
	require.Equal(t, primary.Ts, dataPrepare.Ts)
	require.Contains(t, metaPrepare.ReadKeys, sqsQueueMetaKey(meta.Name))
	require.Contains(t, metaPrepare.ReadKeys, sqsQueueGenKey(meta.Name))
	decision, err := kv.DecodeTxnMeta(primary.Mutations[0].Value)
	require.NoError(t, err)
	require.Greater(t, decision.CommitTS, primary.Ts)
	_, err = f.groups[2].Store.GetAt(ctx, decision.PrimaryKey, decision.CommitTS)
	require.NoError(t, err, "the primary effect must actually be committed before its reply is dropped")
	meta = f.rawMeta(t, ctx)
	require.NotNil(t, meta.DelayReindex)
	require.False(t, meta.FifoOrderIndexed)
	require.Empty(t, meta.DelayReindex.After, "the secondary cursor is still prepared, not independently advanced")
	changed := 0
	for _, id := range ids {
		key := sqsMsgDataKeyDispatch(meta, meta.Name, 0, meta.Generation, id)
		raw, getErr := f.groups[2].Store.GetAt(ctx, key, ^uint64(0))
		require.NoError(t, getErr)
		rec, decodeErr := decodeSQSMessageRecord(raw)
		require.NoError(t, decodeErr)
		if rec.VisibleAtMillis == rec.SendTimestampMillis+900_000 {
			changed++
			require.Contains(t, dataPrepare.ReadKeys, key)
		}
	}
	require.Equal(t, sqsFifoDelayPageSize, changed, "only the first real 2PC page was committed")
	t.Logf("lost primary reply: start_ts=%d commit_ts=%d raft_index=%d rewritten=%d/%d",
		primary.Ts, decision.CommitTS, index, changed, len(ids))

	// Close both real Raft engines and Pebble stores with the secondary still
	// prepared. Recovery must use the persisted primary decision, not the hook.
	f.close()
	f = openSQSDelayMultiGroup(t, dir, false)
	commitPrefix := []byte(kv.TxnKeyPrefix + "cmt|")
	commits, err := f.groups[2].Store.ScanAt(ctx, commitPrefix, prefixScanEnd(commitPrefix), 100, ^uint64(0))
	require.NoError(t, err)
	var commitKey []byte
	for _, pair := range commits {
		if bytes.Equal(kv.ExtractTxnUserKey(pair.Key), decision.PrimaryKey) &&
			binary.BigEndian.Uint64(pair.Key[len(pair.Key)-8:]) == primary.Ts {
			commitKey = bytes.Clone(pair.Key)
			break
		}
	}
	require.NotEmpty(t, commitKey, "the real primary COMMIT record must survive reopening group 2")
	_, routedError := f.shards.GetAt(ctx, commitKey, ^uint64(0))
	t.Logf("persisted primary decision: group=2 key=%q routed_read_error=%v", commitKey, routedError)
	meta, exists, err := f.server.loadQueueMetaAt(ctx, fifoDelayTestQueue, f.server.nextTxnReadTS(ctx))
	require.NoError(t, err, "real read-time resolution must find the committed partition-group primary")
	require.True(t, exists)
	require.NotNil(t, meta.DelayReindex, "one committed page must not complete the remaining backlog")
	require.NotEmpty(t, meta.DelayReindex.After)
	require.False(t, meta.FifoOrderIndexed)
	for _, delay := range []string{"900", "0"} {
		status, out = f.call(t, ctx, sqsSetQueueAttributesTarget, map[string]any{
			"QueueUrl": url, "Attributes": map[string]string{"DelaySeconds": delay},
		})
		require.Equal(t, http.StatusOK, status, out)
		meta = f.rawMeta(t, ctx)
		require.Nil(t, meta.DelayReindex)
		require.True(t, meta.FifoOrderIndexed)
		for _, id := range ids {
			key := sqsMsgDataKeyDispatch(meta, meta.Name, 0, meta.Generation, id)
			raw, getErr := f.shards.GetGroupAt(ctx, 2, key, f.server.nextTxnReadTS(ctx))
			require.NoError(t, getErr)
			rec, decodeErr := decodeSQSMessageRecord(raw)
			require.NoError(t, decodeErr)
			want := rec.SendTimestampMillis
			if delay == "900" {
				want += 900_000
			}
			require.Equal(t, want, rec.VisibleAtMillis, "no record may be skipped after the interrupted cursor")
		}
	}
	for ordinal, id := range ids[:2] {
		status, out = f.call(t, ctx, sqsReceiveMessageTarget, map[string]any{
			"QueueUrl": url, "MaxNumberOfMessages": 10, "VisibilityTimeout": 60, "WaitTimeSeconds": 0,
		})
		require.Equal(t, http.StatusOK, status, out)
		messages, ok := out["Messages"].([]any)
		require.True(t, ok)
		require.Len(t, messages, 1, "recovered receive ordinal=%d must progress", ordinal)
		message := delayNativeMessage(t, messages[0])
		require.Equal(t, id, message["MessageId"], "A must be delivered before B across the recovery boundary")
		status, out = f.call(t, ctx, sqsDeleteMessageTarget, map[string]any{
			"QueueUrl": url, "ReceiptHandle": message["ReceiptHandle"],
		})
		require.Equal(t, http.StatusOK, status, out)
		_, err = f.groups[2].Store.GetAt(ctx, sqsMsgDataKeyDispatch(meta, meta.Name, 0, meta.Generation, id), ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound, "receipt deletion must remove the actual partition owner row, not return a no-op 200")
	}
}
